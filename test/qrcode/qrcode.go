package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"net"

	"github.com/skip2/go-qrcode"
)

// LicenseKey contains the license key information
type LicenseKey struct {
	Username  string
	MacAddr   string
	ExpiresAt time.Time
	Key       string
}

// GenerateQRCode generates a QR code from the content and saves it as an image
func GenerateQRCode(content, filePath string) error {
	qr, err := qrcode.New(content, qrcode.Medium)
	if err != nil {
		return err
	}
	return qr.WriteFile(256, filePath)
}

// DisplayQRCode displays the QR code in the terminal
func DisplayQRCode(content string) error {
	qr, err := qrcode.New(content, qrcode.Medium)
	if err != nil {
		return err
	}
	fmt.Println(qr.ToSmallString(false))
	return nil
}

// GenerateLicenseKey generates a license key based on user ID, software name, and expiration time
func GenerateLicenseKey(userID, software string, expiresAt time.Time, secretKey string) string {
	data := fmt.Sprintf("%s|%s|%d", userID, software, expiresAt.Unix())
	h := hmac.New(sha256.New, []byte(secretKey))
	h.Write([]byte(data))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))
	return fmt.Sprintf("%s|%s", data, signature)
}

// ValidateLicenseKey validates a license key's authenticity and expiration
func ValidateLicenseKey(licenseKey, secretKey string) (bool, string) {
	parts := splitLicenseKey(licenseKey)
	if len(parts) != 4 {
		return false, "invalid license key format"
	}
	data := fmt.Sprintf("%s|%s|%s", parts[0], parts[1], parts[2])
	expectedSignature := generateSignature(data, secretKey)
	if expectedSignature != parts[3] {
		return false, "invalid signature"
	}

	expiresAtUnix, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return false, "invalid expiration timestamp"
	}
	if time.Now().After(time.Unix(expiresAtUnix, 0)) {
		return false, "license key expired"
	}

	return true, "license key is valid"
}

// SplitLicenseKey splits the license key into its components
func splitLicenseKey(licenseKey string) []string {
	return strings.Split(licenseKey, "|")
}

// GenerateSignature generates HMAC SHA-256 signature for data
func generateSignature(data, secretKey string) string {
	h := hmac.New(sha256.New, []byte(secretKey))
	h.Write([]byte(data))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// ScanQRCode scans a QR code from an image file and returns its content
func ScanQRCode(filePath string) (string, error) {
	cmd := exec.Command("zbarimg", filePath)
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return string(output), nil
}

func main() {
	macAddress, err := getDefaultMACAddress()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Printf("Default MAC Address: %s\n", macAddress)
	// // Configurations
	// userID := "user123"
	// software := "mysoftware"
	// expiresAt := time.Now().Add(30 * 24 * time.Hour) // 30 days validity
	// secretKey := "mysecretkey"
	// qrFilePath := "output.png"

	// // Generate QR code content
	// content := fmt.Sprintf("userID=%s&software=%s", userID, software)
	// err := GenerateQRCode(content, qrFilePath)
	// if err != nil {
	// 	log.Fatalf("Failed to generate QR code: %v", err)
	// }

	// // Display QR code on terminal
	// err = DisplayQRCode(content)
	// if err != nil {
	// 	log.Fatalf("Failed to display QR code: %v", err)
	// }

	// fmt.Println("QR code generated and saved as output.png. Please scan the QR code with your phone.")

	// // Simulate scanning the QR code and generating a license key
	// scannedContent, err := ScanQRCode(qrFilePath)
	// if err != nil {
	// 	log.Fatalf("Failed to scan QR code: %v", err)
	// }

	// fmt.Printf("Scanned content: %s\n", scannedContent)

	// // Generate License Key
	// licenseKey := GenerateLicenseKey(userID, software, expiresAt, secretKey)
	// fmt.Printf("Generated License Key: %s\n", licenseKey)

	// // Validate License Key
	// isValid, message := ValidateLicenseKey(licenseKey, secretKey)
	// fmt.Printf("License Key validation: %v (%s)\n", isValid, message)
}

func getDefaultMACAddress() (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, iface := range interfaces {
		// Skip loopback interfaces, virtual interfaces, and interfaces without hardware addresses
		if iface.Flags&net.FlagLoopback != 0 || iface.HardwareAddr == nil {
			continue
		}

		// 检查是否是活跃的接口
		addrs, err := iface.Addrs()
		if err != nil || len(addrs) == 0 {
			continue
		}

		// 返回第一个符合条件的网卡的 MAC 地址
		return iface.HardwareAddr.String(), nil
	}

	return "", fmt.Errorf("no suitable network interface found")
}
