/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"time"

	"github.com/skip2/go-qrcode"
	"github.com/spf13/cobra"
	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/sqlite"
)

const localKey = "wentaojin@20250415#$9568719"

type License struct {
	MacAddress string    `json:"mac_address"`
	UserName   string    `json:"user_name"`
	ExpireTime time.Time `json:"expire_time"`
}

type LicenseKey struct {
	License
	Key string `json:"key"`
}

type AppLicense struct {
	*App
}

func (a *App) AppLicense() Cmder {
	return &AppLicense{App: a}
}

func (a *AppLicense) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "license",
		Short: "License used to for tidba authorization and activation",
		Long:  "License used to for tidba authorization and activation",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cmd.Help(); err != nil {
				return err
			}
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	return cmd
}

type AppLicenseGen struct {
	*AppLicense
	user   string
	day    int
	output string
}

func (a *AppLicense) AppLicenseGen() Cmder {
	return &AppLicenseGen{AppLicense: a}
}

func (a *AppLicenseGen) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate used to for tidba authorization code and QR code",
		Long:  "Generate used to for tidba authorization code and QR code",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if a.user == "" {
				return fmt.Errorf(`the user cannot be empty, required flag(s) --user {userName} not set`)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			macAddr, err := getDefaultMACAddress()
			if err != nil {
				return err
			}

			expir := time.Now().In(time.Local).AddDate(0, 0, a.day)
			lic := License{
				MacAddress: macAddr,
				UserName:   a.user,
				ExpireTime: expir,
			}
			licenseJSON, err := json.Marshal(lic)
			if err != nil {
				return err
			}

			qr, err := qrcode.New(string(licenseJSON), qrcode.Medium)
			if err != nil {
				return err
			}

			filePath := filepath.Join(a.output, "license.png")
			err = qr.WriteFile(256, filePath)
			if err != nil {
				return err
			}

			fmt.Printf("Please contact the author for authorization:\n\n%v\n", qr.ToSmallString(false))
			fmt.Printf("The tidba software license qrcode output: [%v]\n", filePath)
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVar(&a.user, "user", "", "configure the tidba software authorization user name")
	cmd.Flags().IntVar(&a.day, "day", 30, "configure the tidba software license period")
	cmd.Flags().StringVar(&a.output, "outout", "/tmp", "configure the tidba software license qrcode output dir")
	return cmd
}

type AppLicenseActivate struct {
	*AppLicense
	key string
}

func (a *AppLicense) AppLicenseActivate() Cmder {
	return &AppLicenseActivate{AppLicense: a}
}

func (a *AppLicenseActivate) Cmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "activate",
		Short: "Activate used to for tidba activation",
		Long:  "Activate used to for tidba activation",
		RunE: func(cmd *cobra.Command, args []string) error {
			var lic LicenseKey
			decodedKey, err := base64.StdEncoding.DecodeString(a.key)
			if err != nil {
				return err
			}
			err = json.Unmarshal(decodedKey, &lic)
			if err != nil {
				return err
			}

			if time.Now().In(time.Local).After(lic.ExpireTime) {
				fmt.Println("The license has expired and cannot be activated.")
				return nil
			}

			// verify MAC
			macAddr, err := getDefaultMACAddress()
			if err != nil {
				return err
			}
			if macAddr != lic.MacAddress {
				fmt.Println("The server address does not match. Please use the original server address to activate or re-authenticate.")
				return nil
			}

			// verify local passwd
			decodedKey, err = base64.StdEncoding.DecodeString(lic.Key)
			if err != nil {
				return nil
			}
			if string(decodedKey)[:len(localKey)] != localKey {
				fmt.Println("The license key does not match and cannot be activated.")
				return nil
			}

			db, err := database.Connector.GetDatabase(database.DefaultSqliteClusterName)
			if err != nil {
				return err
			}

			meta := db.(*sqlite.Database)
			expirStr := lic.ExpireTime.In(time.Local).Format("2006-01-02 15:04:05")
			if _, err = meta.CreateLicense(context.Background(), &sqlite.License{
				Username:   lic.UserName,
				MacAddress: lic.MacAddress,
				ExpireTime: expirStr,
				License:    a.key,
			}); err != nil {
				return err
			}

			js := make(map[string]interface{})
			js["username"] = lic.UserName
			js["expireTime"] = expirStr

			jsPretty, err := json.MarshalIndent(&js, "", " ")
			if err != nil {
				return err
			}
			fmt.Println(string(jsPretty))
			fmt.Println("The software activation successful!")
			return nil
		},
		TraverseChildren: true,
		SilenceErrors:    true,
		SilenceUsage:     true,
	}
	cmd.Flags().StringVar(&a.key, "key", "", "configure the tidba software license key")
	return cmd
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

		addrs, err := iface.Addrs()
		if err != nil || len(addrs) == 0 {
			continue
		}

		return iface.HardwareAddr.String(), nil
	}

	return "", fmt.Errorf("no suitable network interface found")
}
