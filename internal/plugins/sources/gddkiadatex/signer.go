package gddkiadatex

import (
	"crypto"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/beevik/etree"
	dsig "github.com/russellhaering/goxmldsig"
)

// The KPD service authenticates the caller with a WS-Security signature carried
// in the SOAP header: the client certificate travels as a
// BinarySecurityToken, and the Body element is signed with the matching private
// key. There is no other authentication — an unsigned call is rejected with
// "No WS-Security header found".
//
// The layout below is the one the published .NET sample produces (and the one
// the service accepts): exclusive C14N, RSA-SHA256, a reference to the Body's
// wsu:Id, and a SecurityTokenReference pointing at the token.

const (
	wsseNamespace = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
	wsuNamespace  = "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd"
	bodyID        = "_1"
	tokenID       = "BinaryToken1"

	// envelope is the whole request. The namespaces the Body visibly uses are
	// declared ON the Body as well: exclusive canonicalisation of a subtree
	// needs them locally, and the service canonicalises the same way, so both
	// sides hash the same bytes.
	envelope = `<?xml version="1.0" encoding="UTF-8"?>` +
		`<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:u="` + wsuNamespace + `">` +
		`<soap:Header><wsse:Security xmlns:wsse="` + wsseNamespace + `" xmlns:wsu="` + wsuNamespace + `">` +
		`<wsse:BinarySecurityToken EncodingType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-soap-message-security-1.0#Base64Binary" ValueType="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-x509-token-profile-1.0#X509v3" wsu:Id="` + tokenID + `">%s</wsse:BinarySecurityToken>` +
		`</wsse:Security></soap:Header>` +
		`<soap:Body xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:u="` + wsuNamespace + `" u:Id="` + bodyID + `"/></soap:Envelope>`
)

// signer holds the key material used to sign every request.
type signer struct {
	key  *rsa.PrivateKey
	cert *x509.Certificate
}

// newSigner validates the key material once, at construction.
func newSigner(key *rsa.PrivateKey, cert *x509.Certificate) (*signer, error) {
	if key == nil || cert == nil {
		return nil, fmt.Errorf("kpd: key and certificate are required")
	}
	if time.Now().After(cert.NotAfter) {
		return nil, fmt.Errorf("kpd: certificate expired on %s", cert.NotAfter.Format(time.RFC3339))
	}
	return &signer{key: key, cert: cert}, nil
}

// signedEnvelope builds the signed SOAP request. The signature covers the Body
// only (the pull operation has no parameters), which is exactly what the
// service verifies.
func (s *signer) signedEnvelope() ([]byte, error) {
	doc := etree.NewDocument()
	if err := doc.ReadFromString(fmt.Sprintf(envelope, base64.StdEncoding.EncodeToString(s.cert.Raw))); err != nil {
		return nil, fmt.Errorf("kpd: build envelope: %w", err)
	}
	body := doc.Root().FindElement("./Header/Body")
	if body == nil {
		body = doc.Root().SelectElement("Body")
	}
	security := doc.Root().FindElement("./Header/Security")
	if body == nil || security == nil {
		return nil, fmt.Errorf("kpd: envelope shape is wrong (body=%v security=%v)", body != nil, security != nil)
	}

	ctx, err := dsig.NewSigningContext(s.key, [][]byte{s.cert.Raw})
	if err != nil {
		return nil, fmt.Errorf("kpd: signing context: %w", err)
	}
	ctx.Hash = crypto.SHA256
	// The reference URI has to be "#_1", and the Id lives in the wsu namespace,
	// so the attribute name carries its prefix.
	ctx.IdAttribute = "u:Id"
	ctx.Canonicalizer = dsig.MakeC14N10ExclusiveCanonicalizerWithPrefixList("")
	if err := ctx.SetSignatureMethod(dsig.RSASHA256SignatureMethod); err != nil {
		return nil, fmt.Errorf("kpd: signature method: %w", err)
	}

	sig, err := ctx.ConstructSignature(body, true)
	if err != nil {
		return nil, fmt.Errorf("kpd: sign: %w", err)
	}
	// KeyInfo is outside the signature, so the generated X509Data is replaced
	// with the reference form WSS4J expects, pointing at the token above.
	if ki := sig.SelectElement("KeyInfo"); ki != nil {
		ki.Child = nil
		str := ki.CreateElement("SecurityTokenReference")
		str.Space = "wsse"
		ref := str.CreateElement("Reference")
		ref.Space = "wsse"
		ref.CreateAttr("URI", "#"+tokenID)
		ref.CreateAttr("ValueType", "http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-x509-token-profile-1.0#X509")
	}
	security.AddChild(sig)

	out, err := doc.WriteToBytes()
	if err != nil {
		return nil, fmt.Errorf("kpd: serialise envelope: %w", err)
	}
	return out, nil
}
