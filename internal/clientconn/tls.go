// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

package clientconn

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/lazyerrors"
)

type certConfig struct {
	caPath   string
	certPath string
	keyPath  string
}

func generateTLSConfig(c certConfig) (*tls.Config, error) {
	if _, err := os.Stat(c.certPath); err != nil {
		return nil, lazyerrors.Errorf("TLS certificate file does not exist at path %q", c.certPath)
	}
	if _, err := os.Stat(c.keyPath); err != nil {
		return nil, lazyerrors.Errorf("TLS key file does not exist at path %q", c.keyPath)
	}

	cert, err := tls.LoadX509KeyPair(c.certPath, c.keyPath)
	if err != nil {
		return nil, lazyerrors.Errorf("Could not load X.509 key pair: %w", err)
	}
	config := &tls.Config{Certificates: []tls.Certificate{cert}}

	if c.caPath != "" {
		if _, err := os.Stat(c.caPath); err != nil {
			return nil, lazyerrors.Errorf("TLS root CA file does not exist at path %q", c.caPath)
		}

		rootCA, err := os.ReadFile(c.caPath)
		if err != nil {
			return nil, lazyerrors.Errorf("Could not read root CA file: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM(rootCA); !ok {
			return nil, lazyerrors.Errorf("Could not parse root certificate: %w", err)
		}

		config.ClientAuth = tls.RequireAndVerifyClientCert
		config.ClientCAs = caCertPool
	}

	return config, nil
}
