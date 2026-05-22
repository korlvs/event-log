package outbox

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

func ExtractRequestMetadata(r *http.Request) *RequestMetadata {
	meta := &RequestMetadata{
		ClientIP:       getClientIP(r),
		UserAgent:      r.UserAgent(),
		Referer:        r.Referer(),
		AcceptLanguage: r.Header.Get("Accept-Language"),
		CorrelationID:  r.Header.Get("X-Request-ID"),
	}
	userID, email := extractJWTClaims(r)
	meta.UserID = userID
	meta.UserEmail = email
	return meta
}

func getClientIP(r *http.Request) string {
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		ips := strings.Split(forwarded, ",")
		if len(ips) > 0 {
			return strings.TrimSpace(ips[0])
		}
	}
	if realIP := r.Header.Get("X-Real-IP"); realIP != "" {
		return realIP
	}
	return r.RemoteAddr
}

func extractJWTClaims(r *http.Request) (string, string) {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return "", ""
	}
	parts := strings.Split(auth, " ")
	if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
		return "", ""
	}
	tokenString := parts[1]
	parser := jwt.Parser{}
	claims := jwt.MapClaims{}
	_, _, err := parser.ParseUnverified(tokenString, claims)
	if err != nil {
		return "", ""
	}
	var userID, email string
	if sub, ok := claims["sub"].(string); ok {
		userID = sub
	} else if uid, ok := claims["user_id"].(string); ok {
		userID = uid
	} else if uid, ok := claims["user_id"].(float64); ok {
		userID = fmt.Sprintf("%v", uid)
	}
	if em, ok := claims["email"].(string); ok {
		email = em
	}
	return userID, email
}
