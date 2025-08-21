package secrets

import (
	"github.com/golang-jwt/jwt/v5"
)

func GetSecret(token *jwt.Token) (any, error) {
	return []byte("temporary secret"), nil
}
