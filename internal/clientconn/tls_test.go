// SPDX-FileCopyrightText: 2023 SAP SE or an SAP affiliate company
//
// SPDX-License-Identifier: Apache-2.0

package clientconn

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateTLSConfig(t *testing.T) {
	tmpd := os.TempDir()
	caDir := filepath.Join(tmpd, "root.crt")
	privateKeyDir := filepath.Join(tmpd, "tls.key")
	certificateDir := filepath.Join(tmpd, "tls.crt")
	expectedCert, err := generateDummyCerts(tmpd)
	if err != nil {
		t.Fatal(err)
	}

	// valid ca cert and tls certs
	tlsConfig, err := generateTLSConfig(certConfig{
		caPath:   caDir,
		certPath: certificateDir,
		keyPath:  privateKeyDir,
	})
	require.NoError(t, err)
	assert.NotNil(t, tlsConfig)
	assert.Equal(t, tls.RequireAndVerifyClientCert, tlsConfig.ClientAuth)
	pool := x509.NewCertPool()
	expectedCA, err := os.ReadFile(caDir)
	if err != nil {
		t.Fatal(err)
	}
	ok := pool.AppendCertsFromPEM(expectedCA)
	require.True(t, ok)
	assert.Equal(t, pool.Subjects(), tlsConfig.ClientCAs.Subjects())
	// TODO: assert.True(t, pool.Equal(tlsConfig.ClientCAs)), once upgraded to go v1.20
	assert.Len(t, tlsConfig.Certificates, 1)
	assert.Equal(t, expectedCert, tlsConfig.Certificates[0].Certificate[0])

	// valid cert and key
	tlsConfig, err = generateTLSConfig(certConfig{
		certPath: certificateDir,
		keyPath:  privateKeyDir,
	})
	require.NoError(t, err)
	assert.NotNil(t, tlsConfig)
	assert.Equal(t, tls.NoClientCert, tlsConfig.ClientAuth)
	assert.Empty(t, tlsConfig.ClientCAs)
	assert.Len(t, tlsConfig.Certificates, 1)
	assert.Equal(t, expectedCert, tlsConfig.Certificates[0].Certificate[0])

	// valid cert and no key
	tlsConfig, err = generateTLSConfig(certConfig{
		certPath: certificateDir,
		keyPath:  "nonexisting.pem",
	})
	assert.Nil(t, tlsConfig)
	assert.ErrorContains(t, err, "TLS key file does not exist")

	// valid key and no cert
	tlsConfig, err = generateTLSConfig(certConfig{
		certPath: "nonexisting.pem",
		keyPath:  privateKeyDir,
	})
	assert.Nil(t, tlsConfig)
	assert.ErrorContains(t, err, "TLS certificate file does not exist")

	// invalid key and cert
	tlsConfig, err = generateTLSConfig(certConfig{
		certPath: "nonexisting.pem",
		keyPath:  "other-nonexisting.pem",
	})
	assert.Nil(t, tlsConfig)
	assert.ErrorContains(t, err, "file does not exist")

	// cleanup
	_ = os.Remove(caDir)
	_ = os.Remove(privateKeyDir)
	_ = os.Remove(certificateDir)
}

func generateDummyCerts(certDir string) ([]byte, error) {
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, fmt.Errorf("Failed to create root serial number: %w", err)
	}
	rootKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("Failed to create root key: %w", err)
	}

	// generate root CA certificate
	rootTemplate := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Acme Co"},
			CommonName:   "Root CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	rootBytes, err := x509.CreateCertificate(rand.Reader, &rootTemplate, &rootTemplate, &rootKey.PublicKey, rootKey)
	if err != nil {
		return nil, fmt.Errorf("Failed to create root certificate: %w", err)
	}
	certToFile(filepath.Join(certDir, "root.crt"), rootBytes)

	// generate tls key
	tlsKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("Failed to create tls key: %w", err)
	}
	keyToFile(filepath.Join(certDir, "tls.key"), tlsKey)

	// generate tls certificate
	serialNumber, err = rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, fmt.Errorf("Failed to create tls serial number: %w", err)
	}
	tlsTemplate := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Acme Co"},
			CommonName:   "TLS Test Certificate",
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(365 * 24 * time.Hour),

		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
		DNSNames:              []string{"localhost"},
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &tlsTemplate, &rootTemplate, &tlsKey.PublicKey, rootKey)
	if err != nil {
		return nil, fmt.Errorf("Failed to create tls certificate: %w", err)
	}
	certToFile(filepath.Join(certDir, "tls.crt"), derBytes)

	return derBytes, nil
}

func keyToFile(filename string, key *ecdsa.PrivateKey) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	b, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return fmt.Errorf("Unable to marshal ECDSA private key: %w", err)
	}
	if err := pem.Encode(file, &pem.Block{Type: "EC PRIVATE KEY", Bytes: b}); err != nil {
		return err
	}
	return nil
}

func certToFile(filename string, fileContent []byte) error {
	certOut, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to open %s for writing: %w", filename, err)
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: fileContent}); err != nil {
		return fmt.Errorf("failed to write data to %s: %w", filename, err)
	}
	if err := certOut.Close(); err != nil {
		return fmt.Errorf("error closing %s: %w", filename, err)
	}
	return nil
}
