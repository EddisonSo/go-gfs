package secrets

import (
	"github.com/golang-jwt/jwt/v5"
)

/*
 * Retrieves secrets for signing and verifying JWT
 * Notice that it says "temporary secret", DO NOT USE IT FOR PRODUCTION
 */
func GetSecret(token *jwt.Token) (any, error) {
	return []byte("temporary secret"), nil
}
