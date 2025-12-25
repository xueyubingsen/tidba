//go:build nolicense

package cmd

// IsLicenseCheckEnabled returns false when built with -tags nolicense
// so the binary will skip license checks at runtime (useful for tests).
func IsLicenseCheckEnabled() bool { return false }
