// DBDeployer - The MySQL Sandbox
// Copyright © 2006-2022 Giuseppe Maxia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ops

import (
	"os"
	"path"
	"strings"
	"testing"

	"github.com/datacharmer/dbdeployer/common"
	"github.com/datacharmer/dbdeployer/defaults"
	"github.com/datacharmer/dbdeployer/downloads"
	"github.com/stretchr/testify/require"
)

func TestGetTarball(t *testing.T) {

	if os.Getenv("GITHUB_ACTIONS") != "" {
		t.Skip("Not stable running on GitHub")
	}
	sbEnv := os.Getenv("SANDBOX_BINARY")
	sandboxBinary := defaults.Defaults().SandboxBinary

	if sbEnv != "" {
		sandboxBinary = sbEnv
	}

	var newestList = make(map[string]downloads.TarballDescription)

	for _, tb := range downloads.DefaultTarballRegistry.Tarballs {
		t.Run(tb.Name, func(t *testing.T) {
			retrieved, err := findRemoteTarballByNameOrUrl(tb.Name, tb.OperatingSystem)
			require.NoError(t, err)
			require.Equal(t, retrieved, tb)
			//if !strings.EqualFold(tb.OperatingSystem, runtime.GOOS) {
			//	return
			//}
			if !strings.EqualFold(tb.Flavor, common.MySQLFlavor) {
				return
			}
			if strings.EqualFold(tb.OperatingSystem, "linux") && !tb.Minimal {
				return
			}
			latestTb, ok := newestList[tb.ShortVersion]
			if ok {
				latestVersionList, err := common.VersionToList(latestTb.Version)
				require.NoError(t, err)
				isGreater, err := common.GreaterOrEqualVersion(tb.Version, latestVersionList)
				require.NoError(t, err)
				if isGreater {
					newestList[tb.ShortVersion] = tb
				}
			} else {
				newestList[tb.ShortVersion] = tb
			}
		})
	}
	curDir, err := os.Getwd()
	require.NoError(t, err, "failed to get current working directory")

	// Helper function to get absolute path of downloaded file
	getDownloadedPath := func(tb downloads.TarballDescription) string {
		absPath, err := common.AbsolutePath(tb.Name)
		if err != nil {
			// Fallback to current directory if AbsolutePath fails
			return path.Join(curDir, tb.Name)
		}
		return absPath
	}

	// Helper function to clean up file before/after test
	cleanupFile := func(filePath string) {
		if common.FileExists(filePath) {
			_ = os.Remove(filePath)
		}
	}

	for v, tb := range newestList {
		if tb.Flavor != common.MySQLFlavor {
			continue
		}
		//if !strings.EqualFold(tb.OperatingSystem, runtime.GOOS) {
		//	continue
		//}
		t.Run("latest "+v, func(t *testing.T) {
			// First, determine what tarball would be downloaded by version lookup
			minimal := strings.EqualFold(tb.OperatingSystem, "linux")
			expectedTarball, err := findRemoteTarballByVersion(v, tb.Flavor, tb.OperatingSystem, "", minimal, true, false)

			// Skip if version detection fails (older versions may not have matching tarballs)
			if err != nil {
				if strings.Contains(err.Error(), "error detecting latest version") {
					t.Skipf("No matching tarball found for version %s with OS %s and minimal %v", v, tb.OperatingSystem, minimal)
				}
				require.NoError(t, err)
			}

			// Clean up file before test using the expected tarball name
			downloadedPath := getDownloadedPath(expectedTarball)
			cleanupFile(downloadedPath)

			err = GetRemoteTarball(DownloadsOptions{
				SandboxBinary: sandboxBinary,
				TarballOS:     tb.OperatingSystem,
				Flavor:        tb.Flavor,
				Version:       v,
				Newest:        true,
				Minimal:       minimal,
			})

			require.NoError(t, err)
			require.FileExists(t, downloadedPath, "downloaded tarball %s not found", downloadedPath)
			cleanupFile(downloadedPath)
		})
		t.Run("by-name-"+tb.Name, func(t *testing.T) {
			// Clean up file before test
			downloadedPath := getDownloadedPath(tb)
			cleanupFile(downloadedPath)

			err := GetRemoteTarball(DownloadsOptions{
				TarballName: tb.Name,
				TarballUrl:  "",
				TarballOS:   tb.OperatingSystem,
			})
			require.NoError(t, err)
			require.FileExists(t, downloadedPath, "downloaded tarball %s not found", downloadedPath)
			cleanupFile(downloadedPath)
		})
		t.Run("by-URL-"+tb.Name, func(t *testing.T) {
			// Clean up file before test
			downloadedPath := getDownloadedPath(tb)
			cleanupFile(downloadedPath)

			err := GetRemoteTarball(DownloadsOptions{
				TarballName: "",
				TarballUrl:  tb.Url,
			})
			require.NoError(t, err)
			require.FileExists(t, downloadedPath, "downloaded tarball %s not found", downloadedPath)
			cleanupFile(downloadedPath)
		})
	}
}
