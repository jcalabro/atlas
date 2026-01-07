package pds

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
)

// Config represents the TOML configuration file structure
type Config struct {
	Hosts     map[string]Host  `toml:"hosts"`
	Blobstore *BlobstoreConfig `toml:"blobstore"`
}

// BlobstoreConfig contains S3-compatible storage settings
type BlobstoreConfig struct {
	Endpoint  string `toml:"endpoint"`
	Bucket    string `toml:"bucket"`
	Region    string `toml:"region"`
	AccessKey string `toml:"access_key"`
	SecretKey string `toml:"secret_key"`
}

// Host contains configuration for a single PDS hostname
type Host struct {
	ServiceDID     string   `toml:"service_did"`
	JWTSigningKey  string   `toml:"jwt_signing_key"`
	UserDomains    []string `toml:"user_domains"`
	ContactEmail   string   `toml:"contact_email"`
	PrivacyPolicy  string   `toml:"privacy_policy"`
	TermsOfService string   `toml:"terms_of_service"`
}

// loadedHostConfig contains the parsed and validated config for a single host
type loadedHostConfig struct {
	hostname       string
	serviceDID     string
	signingKey     *ecdsa.PrivateKey
	userDomains    []string
	contactEmail   string
	privacyPolicy  string
	termsOfService string
}

// LoadedConfig contains the fully parsed configuration
type LoadedConfig struct {
	Hosts     map[string]*loadedHostConfig
	Blobstore *BlobstoreConfig
}

// LoadConfig reads and parses the TOML config file, loading all signing keys
func LoadConfig(path string) (*LoadedConfig, error) {
	var cfg Config
	if _, err := toml.DecodeFile(path, &cfg); err != nil {
		return nil, fmt.Errorf("failed to decode config file: %w", err)
	}

	if len(cfg.Hosts) == 0 {
		return nil, fmt.Errorf("config must define at least one host")
	}

	hosts := make(map[string]*loadedHostConfig, len(cfg.Hosts))
	for hostname, host := range cfg.Hosts {
		if err := validateHostConfig(hostname, &host); err != nil {
			return nil, fmt.Errorf("invalid config for host %q: %w", hostname, err)
		}

		signingKey, err := loadSigningKey(host.JWTSigningKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load signing key for host %q: %w", hostname, err)
		}

		hosts[hostname] = &loadedHostConfig{
			hostname:       hostname,
			serviceDID:     host.ServiceDID,
			signingKey:     signingKey,
			userDomains:    host.UserDomains,
			contactEmail:   host.ContactEmail,
			privacyPolicy:  host.PrivacyPolicy,
			termsOfService: host.TermsOfService,
		}
	}

	return &LoadedConfig{
		Hosts:     hosts,
		Blobstore: cfg.Blobstore,
	}, nil
}

func validateHostConfig(hostname string, cfg *Host) error {
	switch {
	case hostname == "":
		return fmt.Errorf("hostname cannot be empty")
	case cfg.ServiceDID == "":
		return fmt.Errorf("service_did is required")
	case cfg.JWTSigningKey == "":
		return fmt.Errorf("jwt_signing_key is required")
	case len(cfg.UserDomains) == 0:
		return fmt.Errorf("user_domains is required")
	}
	return nil
}

func loadSigningKey(path string) (*ecdsa.PrivateKey, error) {
	keyBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read signing key file: %w", err)
	}

	block, _ := pem.Decode(keyBytes)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block containing signing key")
	}

	key, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse EC private key: %w", err)
	}

	return key, nil
}
