package cmd

import (
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	systemd "github.com/openfaas/faasd/pkg/systemd"
	"github.com/pkg/errors"

	"github.com/spf13/cobra"
)

var installCmd = &cobra.Command{
	Use:   "install",
	Short: "Install faasd",
	RunE:  runInstall,
}

const workingDirectoryPermission = 0755

const faasdwd = "/var/lib/faasd"
const faasdProviderWd = "/var/lib/faasd-provider"
const gatewayRuntimeEnvFile = "gateway.env"

func runInstall(_ *cobra.Command, _ []string) error {

	if err := ensureWorkingDir(path.Join(faasdwd, "secrets")); err != nil {
		return err
	}

	if err := ensureWorkingDir(faasdProviderWd); err != nil {
		return err
	}

	if basicAuthErr := makeBasicAuthFiles(path.Join(faasdwd, "secrets")); basicAuthErr != nil {
		return errors.Wrap(basicAuthErr, "cannot create basic-auth-* files")
	}

	if err := cp("docker-compose.yaml", faasdwd); err != nil {
		return err
	}

	if err := cp("prometheus.yml", faasdwd); err != nil {
		return err
	}

	if err := cp("resolv.conf", faasdwd); err != nil {
		return err
	}

	if err := writeGatewayEnv(path.Join(faasdwd, gatewayRuntimeEnvFile)); err != nil {
		return errors.Wrap(err, "cannot write gateway env file")
	}

	if err := binExists("/usr/local/bin/", "faasd"); err != nil {
		return err
	}

	if err := binExists("/usr/local/bin/", "faasd-gateway"); err != nil {
		return err
	}

	if err := systemd.InstallUnit("faasd-provider", map[string]string{
		"Cwd":             faasdProviderWd,
		"SecretMountPath": path.Join(faasdwd, "secrets")}); err != nil {
		return err
	}

	if err := systemd.InstallUnit("faasd", map[string]string{"Cwd": faasdwd}); err != nil {
		return err
	}

	if err := systemd.InstallUnit("faasd-gateway", map[string]string{
		"Cwd":            faasdwd,
		"GatewayEnvPath": path.Join(faasdwd, gatewayRuntimeEnvFile),
	}); err != nil {
		return err
	}

	if err := systemd.DaemonReload(); err != nil {
		return err
	}

	if err := systemd.Enable("faasd-provider"); err != nil {
		return err
	}

	if err := systemd.Enable("faasd"); err != nil {
		return err
	}

	if err := systemd.Enable("faasd-gateway"); err != nil {
		return err
	}

	if err := systemd.Start("faasd-provider"); err != nil {
		return err
	}

	if err := systemd.Start("faasd"); err != nil {
		return err
	}

	if err := systemd.Start("faasd-gateway"); err != nil {
		return err
	}

	fmt.Println(`
The initial setup downloads various container images, which may take a 
minute or two depending on your connection.

Check the status of the faasd service with:

  sudo journalctl -u faasd --lines 100 -f
  sudo journalctl -u faasd-provider --lines 100 -f

Gateway logs:

  sudo journalctl -u faasd-gateway --lines 100 -f

Login with:
  sudo -E cat /var/lib/faasd/secrets/basic-auth-password | faas-cli login -s`)

	fmt.Println("")

	return nil
}

func writeGatewayEnv(filePath string) error {
	lines := []string{
		"basic_auth=true",
		"functions_provider_url=http://127.0.0.1:8081/",
		"logs_provider_url=http://127.0.0.1:8081/",
		"direct_functions=false",
		"read_timeout=60s",
		"write_timeout=60s",
		"upstream_timeout=65s",
		"faas_nats_address=127.0.0.1",
		"faas_nats_port=4222",
		"faas_prometheus_host=127.0.0.1",
		"faas_prometheus_port=9090",
		"secret_mount_path=/var/lib/faasd/secrets",
		"scale_from_zero=true",
		"function_namespace=openfaas-fn",
	}

	content := strings.Join(lines, "\n") + "\n"
	return os.WriteFile(filePath, []byte(content), workingDirectoryPermission)
}

func binExists(folder, name string) error {
	findPath := path.Join(folder, name)
	if _, err := os.Stat(findPath); err != nil {
		return fmt.Errorf("unable to stat %s, install this binary before continuing", findPath)
	}
	return nil
}
func ensureSecretsDir(folder string) error {
	if _, err := os.Stat(folder); err != nil {
		err = os.MkdirAll(folder, secretDirPermission)
		if err != nil {
			return err
		}
	}

	return nil
}
func ensureWorkingDir(folder string) error {
	if _, err := os.Stat(folder); err != nil {
		err = os.MkdirAll(folder, workingDirectoryPermission)
		if err != nil {
			return err
		}
	}

	return nil
}

func cp(source, destFolder string) error {
	file, err := os.Open(source)
	if err != nil {
		return err

	}
	defer file.Close()

	out, err := os.Create(path.Join(destFolder, source))
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, file)

	return err
}
