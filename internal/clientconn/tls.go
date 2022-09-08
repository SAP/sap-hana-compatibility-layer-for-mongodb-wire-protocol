// SPDX-FileCopyrightText: 2022 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

package clientconn

import (
	"crypto/tls"

	"github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/internal/util/lazyerrors"
)

func generateX509Cert(certFilePath string, keyFilePath string) (*tls.Config, error) {
	if certFilePath == "" {
		return nil, lazyerrors.Errorf("No path was given for the certificate file for TLS")
	} else if keyFilePath == "" {
		return nil, lazyerrors.Errorf("No path was given for the key file for TLS")
	}

	cert, err := tls.LoadX509KeyPair(certFilePath, keyFilePath)
	if err != nil {
		return nil, lazyerrors.Errorf("Following error occured when loading the x509 key and cert files: %w", err)
	}
	config := &tls.Config{Certificates: []tls.Certificate{cert}}

	return config, err
}
