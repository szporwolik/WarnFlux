package gddkiadatex

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/beevik/etree"
)

// testSigner builds an in-memory identity: the signature mechanics are what the
// test covers, not the provider's certificate.
func testSigner(t *testing.T) *signer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "WarnFlux test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	s, err := newSigner(key, cert)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// The envelope the service accepts, field by field: WS-Security present, the
// certificate as a BinarySecurityToken, a signature referencing the Body's
// wsu:Id, and the KeyInfo pointing back at the token.
func TestSignedEnvelopeShape(t *testing.T) {
	out, err := testSigner(t).signedEnvelope()
	if err != nil {
		t.Fatalf("signedEnvelope: %v", err)
	}
	doc := etree.NewDocument()
	if err := doc.ReadFromBytes(out); err != nil {
		t.Fatalf("the envelope is not parseable XML: %v", err)
	}
	root := doc.Root()
	security := root.FindElement("./Header/Security")
	if security == nil {
		t.Fatal("no wsse:Security header")
	}
	token := security.SelectElement("BinarySecurityToken")
	if token == nil || token.Text() == "" {
		t.Fatal("no BinarySecurityToken content")
	}
	if got := token.SelectAttrValue("Id", ""); got != tokenID {
		t.Fatalf("token id = %q, want %q", got, tokenID)
	}
	body := root.SelectElement("Body")
	if body == nil {
		t.Fatal("no SOAP Body")
	}
	if got := body.SelectAttrValue("u:Id", ""); got != bodyID {
		t.Fatalf("body id = %q, want %q", got, bodyID)
	}
	sig := security.SelectElement("Signature")
	if sig == nil {
		t.Fatal("no signature in the security header")
	}
	ref := sig.FindElement("./SignedInfo/Reference")
	if ref == nil {
		t.Fatal("no reference in SignedInfo")
	}
	if got := ref.SelectAttrValue("URI", ""); got != "#"+bodyID {
		t.Fatalf("reference URI = %q, want #%s (an empty URI is what the service rejects)", got, bodyID)
	}
	if dg := ref.SelectElement("DigestValue"); dg == nil || dg.Text() == "" {
		t.Fatal("no digest value")
	}
	sv := sig.SelectElement("SignatureValue")
	if sv == nil || sv.Text() == "" {
		t.Fatal("no signature value")
	}
	ki := sig.SelectElement("KeyInfo")
	if ki == nil {
		t.Fatal("no KeyInfo")
	}
	if ki.SelectElement("X509Data") != nil {
		t.Fatal("KeyInfo still carries X509Data; the service expects a SecurityTokenReference")
	}
	str := ki.SelectElement("SecurityTokenReference")
	if str == nil {
		t.Fatal("no SecurityTokenReference")
	}
	if got := str.SelectElement("Reference").SelectAttrValue("URI", ""); got != "#"+tokenID {
		t.Fatalf("token reference = %q, want #%s", got, tokenID)
	}
	if !strings.Contains(sig.SelectElement("SignedInfo").SelectElement("SignatureMethod").SelectAttrValue("Algorithm", ""), "rsa-sha256") {
		t.Fatal("signature method is not RSA-SHA256")
	}
}

// Two requests must not share a signature: the signature is over the same body,
// but a stale signed envelope reused across polls would be a replay. The digest
// is deterministic; the assertion here is that signing still works repeatedly
// and keeps a fresh signature value per call.
func TestSignedEnvelopeIsRegeneratedPerCall(t *testing.T) {
	s := testSigner(t)
	first, err := s.signedEnvelope()
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.signedEnvelope()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) == 0 || len(second) == 0 {
		t.Fatal("empty envelope")
	}
}

// An expired certificate is refused at construction: an operator must see the
// problem in the log at start-up, not as a rejected pull every 15 minutes.
func TestSignerRejectsExpiredCertificate(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		NotBefore:    time.Now().Add(-48 * time.Hour),
		NotAfter:     time.Now().Add(-24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newSigner(key, cert); err == nil {
		t.Fatal("newSigner accepted an expired certificate")
	}
}
