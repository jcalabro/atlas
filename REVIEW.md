# Atlas PDS: Comprehensive Review & Roadmap

## Executive Summary

Atlas currently implements **37 endpoints** (including a catch-all proxy) out of roughly **60+ endpoints** a fully-featured PDS needs. The core record lifecycle (create, read, update, delete, list), sync protocol, firehose, sessions, and basic account creation all work. However, there are **critical bugs** in tenant isolation and security, **missing features** that block real-world production use (OAuth, account management, repo import), and **spec compliance gaps** that could cause issues with relays and clients.

This document is organized into:
1. **Bugs & Security Issues** (things that are broken right now)
2. **Spec Compliance Gaps** (places we deviate from the AT Protocol spec)
3. **Missing Endpoints** (full gap analysis vs reference PDS)
4. **Test Coverage Gaps**
5. **Code Quality & Observability Issues**
6. **Prioritized Roadmap**

---

## 1. Bugs & Security Issues

### 1.1 CRITICAL: No Blob Upload Size Limit (OOM Vulnerability)

**File:** `internal/pds/blob.go:70`

```go
data, err := io.ReadAll(r.Body)
```

The blob upload handler reads the entire request body into memory with no size cap. An attacker can send a multi-GB body and crash the server. The ATProto spec recommends a 1MB limit for most blobs (up to 1MB for avatars/banners).

**Fix:** Wrap `r.Body` with `http.MaxBytesReader(w, r.Body, maxBlobSize)` before reading. A 1MB default is reasonable.

### 1.2 CRITICAL: Read Endpoints Don't Enforce Multi-Tenant Isolation

**File:** `internal/pds/repo.go:50-142`, `internal/pds/sync.go` (multiple handlers)

The following unauthenticated endpoints allow reading data from ANY DID, regardless of which host the request came in on:

- `handleGetRecord` - can read records from users on other tenants
- `handleListRecords` - same
- `handleDescribeRepo` - same
- `handleGetBlocks` - same
- `handleGetRepo` - same
- `handleSyncGetRecord` - same
- `handleGetLatestCommit` - same
- `handleGetRepoStatus` - same
- `handleListBlobs` / `handleGetBlob` - same

Per AGENTS.md: "if a user is hosted on `a.atlaspds.net`, their data should never appear in queries to `b.atlaspds.net`." This is violated.

The only read endpoint that correctly scopes to the host is `handleListRepos`, which filters by `host.hostname`.

**Fix:** After resolving a DID, check that the actor's `PdsHost` matches the current request host. Return 404 if mismatched.

### 1.3 HIGH: Refresh Token List Grows Without Bound

**File:** `internal/pds/session.go:180-184`

Every `createSession` call appends a refresh token to the actor's `RefreshTokens` slice, but expired tokens are never purged. For an active user who logs in daily, this list grows ~52 entries/year. The entire actor is serialized as a protobuf and stored in a single FDB key-value pair. FDB has a 100KB value size limit. Eventually, writes will fail.

**Fix:** Before appending a new refresh token, filter out expired tokens from the list.

### 1.4 HIGH: Race Condition in handleRefreshSession / handleDeleteSession

**File:** `internal/pds/session.go:297-351`, `session.go:353-397`

Both handlers read the actor from context (loaded during `authMiddleware`), modify `actor.RefreshTokens` in memory, then save back via `s.db.SaveActor`. Between the auth middleware read and the save, another concurrent request (another refresh, delete, or even a new login) could modify the same actor. This is a classic read-modify-write race that causes lost updates.

**Fix:** These operations should be performed inside a single FDB transaction, or use a compare-and-set pattern (e.g., check actor version/rev before saving).

### 1.5 HIGH: handleCreateSession Returns 400 for Wrong Password (Should Be 401)

**File:** `internal/pds/session.go:100-103`

When the password is wrong, the code returns `s.badRequest()` (400). When the user is not found, it returns `s.unauthorized()` (401). This inconsistency lets attackers distinguish "valid user, wrong password" from "user doesn't exist," enabling account enumeration.

**Fix:** Both cases should return `s.unauthorized()` with the same generic message.

### 1.6 HIGH: Email Verification Code Uses Non-Cryptographic PRNG

**File:** `internal/util/rand.go:7-15`, used at `internal/pds/account.go:125`

`RandString` uses `math/rand/v2`, which is not cryptographically secure. The verification code is `fmt.Sprintf("%s-%s", RandString(6), RandString(6))` = 12 alphanumeric chars. With a predictable PRNG, this is guessable.

**Fix:** Use `crypto/rand` for generating verification codes.

### 1.7 MEDIUM: WriteTimeout Breaks WebSocket Connections

**File:** `internal/pds/server.go:221`

```go
srv := &http.Server{
    WriteTimeout: args.WriteTimeout,
}
```

If `WriteTimeout` is set, it applies globally including to WebSocket connections for `subscribeRepos`. This will forcibly close all firehose subscribers after the timeout expires.

**Fix:** Either don't set `WriteTimeout` on the server (rely on per-handler deadlines), or exempt WebSocket connections from the global timeout.

### 1.8 MEDIUM: TOCTOU Race in applyWrites Conflict Check

**File:** `internal/pds/repo.go:698-713`

The conflict check for creates in `applyWrites` happens BEFORE the FDB transaction:

```go
// check if any creates would conflict with existing records
for i, op := range ops {
    if op.Action == "create" {
        existing, err := s.db.GetRecord(ctx, uri)
        ...
    }
}
// apply all writes atomically
result, err := s.db.ApplyWrites(ctx, actor, ops, in.SwapCommit)
```

A record could be created by another request between the check and the transaction, leading to duplicate records in the MST.

**Fix:** Move the conflict check inside the FDB transaction in `db.ApplyWrites`.

### 1.9 MEDIUM: $type Field Mismatch Not Rejected

**File:** `internal/pds/repo.go:280-282`

```go
if recordData["$type"] == nil || recordData["$type"] == "" {
    recordData["$type"] = in.Collection
}
```

If `$type` IS present but does NOT match the collection, the record is stored with the wrong `$type`. Per spec, this should be rejected.

**Fix:** Add validation: if `$type` is present and non-empty, it MUST match `in.Collection`.

### 1.10 MEDIUM: Firehose Replay-to-Live Gap

**File:** `internal/pds/firehose.go:218-233`

When a subscriber connects with a cursor, events are replayed first, then the subscriber is registered for live events. Events arriving between the end of replay and registration are missed.

**Fix:** Register the subscriber BEFORE starting replay. Buffer live events during replay, then drain the buffer after replay completes before switching to live delivery.

### 1.11 MEDIUM: Identity/Account Events Silently Swallowed on Write Failure

**File:** `internal/pds/account.go:158-172`

If writing identity/account events fails, the error is logged but the account creation still succeeds. The relay/firehose never learns about this account.

**Fix:** Either make event writes part of the account creation transaction (preferred), or return an error and roll back account creation.

### 1.12 LOW: Firehose `Since` Field Set to Empty String for Genesis Commits

**File:** `internal/pds/firehose.go:438`

```go
Since: &event.Since,
```

For the first commit, `event.Since` is `""`. Per spec, `since` should be null/absent for genesis commits, not an empty string pointer.

**Fix:** Only set `Since` if `event.Since != ""`.

### 1.13 LOW: Slow Firehose Subscribers Get Events Dropped Silently

**File:** `internal/pds/firehose.go:158-165`

When a subscriber's buffer is full, events are dropped with a log warning but the subscriber is never notified. The subscriber continues with gaps in its event stream.

**Fix:** Close slow subscriber connections (or send an error frame) so they know to reconnect with a cursor.

### 1.14 LOW: No Rate Limiting on createSession

**File:** `internal/pds/session.go:33`

No rate limiting on login attempts. Unlimited brute-force is possible.

### 1.15 LOW: Storing Full Refresh JWT Strings in Database

**File:** `internal/pds/session.go:181`

If FDB is compromised, an attacker gets valid refresh tokens. Best practice is to store a hash of the token and compare hashes on verification.

---

## 2. Spec Compliance Gaps

### 2.1 XRPC Error Names Not Specific Enough

**File:** `internal/pds/server.go:292-314`

The XRPC spec defines specific error names for specific endpoints. Atlas uses generic HTTP-status-based names:

| Situation | Atlas Returns | Spec Expects |
|-----------|---------------|--------------|
| Handle taken (createAccount) | `InvalidRequest` | `HandleNotAvailable` |
| Wrong credentials (createSession) | `InvalidRequest` | `AuthenticationRequired` |
| Record CID mismatch (put/delete) | `Conflict` | `InvalidSwap` |
| Concurrent modification | `Conflict` | `ConcurrentModification` (or retry) |

### 2.2 No Lexicon-Based Record Validation

The `validate` parameter on createRecord/putRecord/applyWrites is accepted but ignored. No actual schema validation occurs. Records with invalid shapes for their collection type are accepted and stored. While many PDSes skip validation, the spec recommends it and the `validationStatus` field in the response is hardcoded to `"valid"` regardless.

### 2.3 Handle Subdomain Resolution Likely Broken

**File:** `internal/pds/wellknown.go:46-82`

The `handleAtprotoDid` handler tries to detect user handle subdomains, but the `hostMiddleware` has already validated that the request host matches a configured hostname. If a request comes in on `alice.pds1.dev.atlaspds.net`, the `hostMiddleware` needs to match it to a configured host. The current `getHost` does exact matching, so subdomain handle requests likely get rejected at the middleware level before reaching the handler.

**Fix:** The `hostMiddleware` needs to support wildcard/suffix matching for user domains, or the host config needs entries for each user domain pattern.

### 2.4 OAuth Metadata Advertised But Endpoints Not Implemented

**File:** `internal/pds/wellknown.go:137-174`

The `/.well-known/oauth-authorization-server` endpoint returns full metadata advertising OAuth endpoints (`/oauth/authorize`, `/oauth/token`, `/oauth/par`, etc.), but none of these endpoints exist. OAuth clients that discover these endpoints will fail with 404s.

**Fix:** Either remove the OAuth authorization server metadata (keep only `oauth-protected-resource`), or implement the OAuth endpoints. If Atlas delegates to an external authorization server, the `oauth-protected-resource` endpoint should point to that server.

### 2.5 Missing `getServiceAuth` Endpoint

**File:** router in `internal/pds/server.go:338-407`

`com.atproto.server.getServiceAuth` is not implemented. This endpoint is needed for clients to obtain service auth tokens for calling other services (e.g., feed generators, labelers) on behalf of the user. Currently, service auth is only used internally for the appview proxy.

### 2.6 Blob Not Associated with Records

There is no tracking of which records reference which blobs. This means:
- Orphaned blobs are never cleaned up
- Users can upload blobs without ever referencing them, consuming storage indefinitely
- No validation that blob references in records point to valid blobs owned by that user

---

## 3. Missing Endpoints (Gap Analysis)

### 3.1 Endpoints Atlas Has (37)

| Category | Endpoint | Status |
|----------|----------|--------|
| Health | `GET /ping` | Implemented |
| Health | `GET /xrpc/_health` | Implemented |
| Health | `GET /` | Implemented |
| Health | `GET /robots.txt` | Implemented |
| Well-Known | `GET /.well-known/did.json` | Implemented |
| Well-Known | `GET /.well-known/atproto-did` | Implemented (but see 2.3) |
| Well-Known | `GET /.well-known/oauth-protected-resource` | Implemented |
| Well-Known | `GET /.well-known/oauth-authorization-server` | Stub (see 2.4) |
| Identity | `GET com.atproto.identity.resolveHandle` | Implemented |
| Server | `GET com.atproto.server.describeServer` | Implemented |
| Server | `POST com.atproto.server.createAccount` | Implemented |
| Server | `POST com.atproto.server.createSession` | Implemented (bugs noted) |
| Server | `GET com.atproto.server.getSession` | Implemented |
| Server | `POST com.atproto.server.refreshSession` | Implemented (bugs noted) |
| Server | `POST com.atproto.server.deleteSession` | Implemented (bugs noted) |
| Repo | `GET com.atproto.repo.describeRepo` | Implemented |
| Repo | `GET com.atproto.repo.getRecord` | Implemented |
| Repo | `GET com.atproto.repo.listRecords` | Implemented |
| Repo | `POST com.atproto.repo.createRecord` | Implemented |
| Repo | `POST com.atproto.repo.putRecord` | Implemented |
| Repo | `POST com.atproto.repo.deleteRecord` | Implemented |
| Repo | `POST com.atproto.repo.applyWrites` | Implemented |
| Repo | `POST com.atproto.repo.uploadBlob` | Implemented (bugs noted) |
| Sync | `GET com.atproto.sync.listRepos` | Implemented |
| Sync | `GET com.atproto.sync.listBlobs` | Implemented |
| Sync | `GET com.atproto.sync.getBlob` | Implemented |
| Sync | `GET com.atproto.sync.getBlocks` | Implemented |
| Sync | `GET com.atproto.sync.getRecord` | Implemented |
| Sync | `GET com.atproto.sync.getLatestCommit` | Implemented |
| Sync | `GET com.atproto.sync.getRepoStatus` | Implemented |
| Sync | `GET com.atproto.sync.getRepo` | Implemented |
| Sync | `WS com.atproto.sync.subscribeRepos` | Implemented (bugs noted) |
| Feed | `GET app.bsky.feed.getFeed` | Proxy to appview |
| Preferences | `GET app.bsky.actor.getPreferences` | Implemented |
| Preferences | `POST app.bsky.actor.putPreferences` | Implemented |
| Labels | `GET com.atproto.label.queryLabels` | Stub (returns empty) |
| Proxy | `GET/POST /xrpc/*` | Catch-all proxy |

### 3.2 Missing Endpoints (Ordered by Priority)

#### P0: Required for a Working Production PDS

| Endpoint | Why It's Needed |
|----------|----------------|
| `POST com.atproto.identity.updateHandle` | Users can't change their handle. Required for account portability. |
| `GET com.atproto.server.getServiceAuth` | Clients need this to call feed generators, labelers, etc. on behalf of the user. Without it, custom feeds and other service integrations don't work when the client talks directly to the PDS. |
| `POST com.atproto.repo.importRepo` | Required for account migration. Users can't move their repo to Atlas from another PDS. |
| `POST com.atproto.server.requestPasswordReset` | Users can't reset their password if they forget it. |
| `POST com.atproto.server.resetPassword` | Companion to requestPasswordReset. |
| `POST com.atproto.server.deactivateAccount` | Users can't deactivate their account. |
| `POST com.atproto.server.activateAccount` | Users can't reactivate after deactivation. |
| `POST com.atproto.server.deleteAccount` | Users can't delete their account. Required by many jurisdictions (GDPR). |
| `POST com.atproto.server.requestAccountDelete` | Companion to deleteAccount. |

#### P1: Needed for Full ATProto Compliance

| Endpoint | Why It's Needed |
|----------|----------------|
| `POST com.atproto.server.requestEmailConfirmation` | Email verification flow. |
| `POST com.atproto.server.confirmEmail` | Email verification flow. |
| `POST com.atproto.server.requestEmailUpdate` | Users can't change their email. |
| `POST com.atproto.server.updateEmail` | Users can't change their email. |
| `POST com.atproto.server.createAppPassword` | App passwords enable granular access control for third-party apps. |
| `GET com.atproto.server.listAppPasswords` | Companion to createAppPassword. |
| `POST com.atproto.server.revokeAppPassword` | Companion to createAppPassword. |
| `GET com.atproto.server.checkAccountStatus` | Allows users to check their account status. |
| `GET com.atproto.server.reserveSigningKey` | Needed for account migration to Atlas. |
| `GET com.atproto.identity.getRecommendedDidCredentials` | Needed for account migration. |
| `POST com.atproto.identity.requestPlcOperationSignature` | Needed for DID operations (handle updates, key rotation). |
| `POST com.atproto.identity.signPlcOperation` | Needed for DID operations. |
| `POST com.atproto.identity.submitPlcOperation` | Needed for DID operations. |
| `POST com.atproto.moderation.createReport` | Users can't report content from the PDS. |
| `GET com.atproto.repo.listMissingBlobs` | Needed for repo integrity checking after import. |

#### P2: OAuth (Needed for Modern Client Compatibility)

| Endpoint | Why It's Needed |
|----------|----------------|
| `POST /oauth/par` | Pushed Authorization Request. Required by ATProto OAuth spec. |
| `GET /oauth/authorize` | User-facing authorization page. |
| `POST /oauth/token` | Token exchange. |
| `POST /oauth/revoke` | Token revocation. |
| `GET /oauth/jwks` | Public key set. |
| DPoP proof validation | Resource server must validate DPoP-bound tokens. |

OAuth is not yet strictly required (legacy sessions still work), but the ecosystem is moving toward it. Third-party OAuth clients already fail against Atlas because the metadata advertises endpoints that don't exist. Short term: either remove the misleading metadata or implement OAuth. Long term: full OAuth implementation is necessary.

#### P3: Admin Endpoints

| Endpoint | Why It's Needed |
|----------|----------------|
| `POST com.atproto.admin.deleteAccount` | Admin account deletion. |
| `GET com.atproto.admin.getAccountInfo` | Admin account inspection. |
| `GET com.atproto.admin.getAccountInfos` | Batch admin inspection. |
| `POST com.atproto.admin.updateAccountEmail` | Admin email updates. |
| `POST com.atproto.admin.updateAccountHandle` | Admin handle updates. |
| `POST com.atproto.admin.updateAccountPassword` | Admin password resets. |
| `POST com.atproto.admin.updateSubjectStatus` | Takedown/suspension support. |
| `GET com.atproto.admin.getSubjectStatus` | Check moderation status. |
| `POST com.atproto.admin.sendEmail` | Admin email sending. |

---

## 4. Test Coverage Gaps

### 4.1 What's Well-Tested

- Account creation (happy path, validation, duplicate handle/email)
- Session CRUD (create, get, refresh, delete, expiration, multi-session)
- Record CRUD (create, get, list, put, delete, applyWrites)
- Sync endpoints (getRepo, getBlocks, getLatestCommit, getRepoStatus, getRecord with CAR format)
- Blob operations (upload, list, get)
- Firehose basics (WebSocket connect, event generation, cursor replay)
- Feed proxying and preferences
- TID generation (including concurrent)

### 4.2 What's Not Tested

| Area | Gap |
|------|-----|
| **Multi-tenancy** | No tests verify host isolation. No tests with multiple configured hosts. This is arguably the most important architectural feature. |
| **Concurrent record operations** | No tests for concurrent creates/deletes to the same repo. No tests for the ConcurrentModification error path under real contention. |
| **Refresh token race conditions** | No tests for concurrent refresh/delete session calls. |
| **Configuration loading** | No tests for config parsing, validation, or reload (SIGHUP). |
| **Error recovery** | No tests for FDB transaction failures, S3 failures, or partial write recovery. |
| **Firehose advanced scenarios** | No tests for slow subscribers (buffer overflow/drop), long-running connections, multiple concurrent subscribers, or the replay-to-live transition gap. |
| **WebSocket timeout** | No tests verifying WebSocket connections survive past WriteTimeout. |
| **Blob edge cases** | No tests for oversized blobs (currently no limit!), concurrent uploads, or content-type validation. |
| **HandleAtprotoDid** | No tests for subdomain handle resolution via `/.well-known/atproto-did`. |
| **Proxy error handling** | No tests for upstream failures, timeouts, or error propagation. |
| **Service auth** | No direct tests for service auth token generation. |

---

## 5. Code Quality & Observability Issues

### 5.1 Observability

**Good:**
- Comprehensive Prometheus metrics (requests, durations, account creations, auth attempts, record operations, blob uploads/downloads, firehose events)
- Full OpenTelemetry tracing on all handlers
- Structured logging with slog

**Gaps:**
- No metric for active WebSocket connections (only subscriber count per host)
- No metric for FDB transaction retries or latency
- No alerting thresholds documented
- No metric for actor protobuf serialization size (would catch the refresh token growth issue)
- No request body size metrics

### 5.2 Code Quality

**Good:**
- Clean separation of concerns (handlers, middleware, db)
- Consistent error wrapping with context
- Proper use of `t.Parallel()` in tests
- Good use of FDB transactions for atomicity

**Issues:**
- `actorFromContext` returns nil but callers always check nil and return `internalErr` - this pattern repeats in every handler. Consider a helper or middleware that guarantees the actor is present.
- Several handlers call `span.End()` but the span was already started by the observability middleware. Double-ending spans is harmless but misleading. The explicit `spanFromContext` + `span.End()` in handlers like `handleGetRecord` is unnecessary.
- The `createSession` function mutates the actor in place (appending refresh tokens) and then saves it. This mutation-then-save pattern without transactional protection is error-prone. It would be cleaner to have the DB layer handle the append atomically.
- `handleDeleteSession` ends without writing a response on success (implicit 200 with empty body). This works but is fragile.

---

## 6. Prioritized Roadmap

### Phase 1: Fix Critical Bugs (Do First)

1. **Add blob upload size limit** (`http.MaxBytesReader`, 1MB default)
2. **Add multi-tenant isolation to read endpoints** (check actor.PdsHost == host.hostname)
3. **Fix createSession to return 401 for wrong password** (not 400)
4. **Fix $type validation** (reject mismatches, don't silently override)
5. **Fix refresh token list growth** (prune expired tokens before appending)
6. **Fix firehose `Since` field** (nil for genesis commits, not empty string)
7. **Use crypto/rand for email verification codes**
8. **Fix or remove WriteTimeout to avoid killing WebSocket connections**

### Phase 2: Core Missing Features (Required for Production)

1. **Password reset flow** (requestPasswordReset, resetPassword) - requires SMTP integration
2. **Account lifecycle** (deactivateAccount, activateAccount, deleteAccount, requestAccountDelete)
3. **Handle updates** (identity.updateHandle)
4. **getServiceAuth endpoint**
5. **Repo import** (importRepo) - required for account migration
6. **Email verification** (requestEmailConfirmation, confirmEmail)
7. **Email update** (requestEmailUpdate, updateEmail)

### Phase 3: Fix Race Conditions & Harden

1. **Atomic refresh token operations** (move refresh/delete session to FDB transactions)
2. **Move applyWrites conflict check inside FDB transaction**
3. **Fix firehose replay-to-live gap** (register subscriber before replay)
4. **Close slow firehose subscribers** instead of silently dropping events
5. **Make identity/account event writes transactional** with account creation
6. **Add rate limiting to createSession** (at minimum, per-IP)
7. **Fix handle subdomain resolution** in hostMiddleware

### Phase 4: OAuth

1. **Short term:** Fix the misleading `oauth-authorization-server` metadata. Either remove it or point `oauth-protected-resource` to an external authorization server.
2. **Implement DPoP validation** on the resource server side (accept DPoP-bound tokens alongside legacy Bearer tokens)
3. **Implement full OAuth authorization server:** PAR, authorize, token, revoke, JWKS endpoints with DPoP nonce management and PKCE

### Phase 5: App Passwords & Admin

1. **App passwords** (createAppPassword, listAppPasswords, revokeAppPassword)
2. **Account status** (checkAccountStatus)
3. **PLC operations** (getRecommendedDidCredentials, requestPlcOperationSignature, signPlcOperation, submitPlcOperation)
4. **Moderation** (createReport)
5. **Admin endpoints** (getAccountInfo, updateAccountHandle, updateAccountEmail, updateSubjectStatus, etc.)
6. **Signing key reservation** (reserveSigningKey)
7. **listMissingBlobs** (for repo integrity after import)

### Phase 6: Polish & Scale

1. **Lexicon-based record validation** (optional but recommended)
2. **Blob-record association tracking** and garbage collection
3. **FDB-backed identity directory cache** (replace in-memory cache)
4. **Invite code system** (if desired)
5. **Comprehensive multi-tenant integration tests**
6. **Load/stress testing**
7. **Blob content-type validation** and XSS prevention

---

## Appendix: Reference Implementations Consulted

- **TypeScript reference PDS:** `/home/jcalabro/go/src/github.com/bluesky-social/atproto/packages/pds/`
- **Cocoon (Go PDS by Hailey):** `/home/jcalabro/go/src/github.com/haileyok/cocoon/`
- **Indigo (Go AT Protocol library):** `/home/jcalabro/go/src/github.com/bluesky-social/indigo/`
- **Entryway (Bluesky OAuth server):** `/home/jcalabro/go/src/github.com/bluesky-social/entryway/`
- **AT Protocol Spec:** https://atproto.com/specs/
