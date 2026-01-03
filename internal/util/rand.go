package util

import "math/rand/v2"

// Generates a random string of basic ASCII western letters and numbers of the given length
// using a non-cryptographically secure PRNG
func RandString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"

	str := ""
	for range length {
		str += string(charset[rand.IntN(len(charset))])
	}
	return str
}
