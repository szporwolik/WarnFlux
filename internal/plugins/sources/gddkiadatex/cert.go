package gddkiadatex

import (
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	pkcs12 "software.sslmate.com/src/go-pkcs12"
)

// loadKeyPair reads the client identity the way operators actually keep it: a
// PKCS#12 file from the provider (what GDDKiA hands out), or a PEM pair.
func loadKeyPair(certFile, keyFile, password string) (*rsa.PrivateKey, *x509.Certificate, error) {
	certFile = strings.TrimSpace(certFile)
	keyFile = strings.TrimSpace(keyFile)
	if certFile == "" {
		return nil, nil, fmt.Errorf("kpd: cert_file is required")
	}
	raw, err := os.ReadFile(certFile)
	if err != nil {
		return nil, nil, fmt.Errorf("kpd: read %s: %w", certFile, err)
	}
	if keyFile != "" {
		return loadPEM(raw, keyFile)
	}
	key, cert, err := pkcs12.Decode(raw, password)
	if err != nil {
		return nil, nil, fmt.Errorf("kpd: decode %s (cert_password wrong?): %w", certFile, err)
	}
	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, nil, fmt.Errorf("kpd: %s does not hold an RSA key", certFile)
	}
	if cert == nil {
		return nil, nil, fmt.Errorf("kpd: %s holds no leaf certificate", certFile)
	}
	return rsaKey, cert, nil
}

// loadPEM reads a certificate and its key from two PEM files.
func loadPEM(certPEM []byte, keyFile string) (*rsa.PrivateKey, *x509.Certificate, error) {
	keyRaw, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("kpd: read %s: %w", keyFile, err)
	}
	pair, err := tls.X509KeyPair(certPEM, keyRaw)
	if err != nil {
		return nil, nil, fmt.Errorf("kpd: load key pair: %w", err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, nil, fmt.Errorf("kpd: parse certificate: %w", err)
	}
	key, ok := pair.PrivateKey.(*rsa.PrivateKey)
	if !ok {
		return nil, nil, fmt.Errorf("kpd: %s does not hold an RSA key", keyFile)
	}
	return key, leaf, nil
}

// CertificateInfo is what the panel and the logs may show about the identity
// without exposing the key (subject and validity).
type CertificateInfo struct {
	Subject   string
	NotBefore time.Time
	NotAfter  time.Time
}

// info summarises the certificate.
func info(cert *x509.Certificate) CertificateInfo {
	if cert == nil {
		return CertificateInfo{}
	}
	return CertificateInfo{Subject: cert.Subject.String(), NotBefore: cert.NotBefore, NotAfter: cert.NotAfter}
}
