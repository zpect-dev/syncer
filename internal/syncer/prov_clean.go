package syncer

import (
	"regexp"
	"strings"
)

// Reglas de limpieza de `prov_des`. Se aplican iterativamente en cada paso:
//  1. Quitar comillas dobles.
//  2. Si hay paréntesis, usar SOLO el contenido del primer paréntesis
//     (ej: "JAIME GOMEZ (EL SURTIDOR DEL LENTE)" -> "EL SURTIDOR DEL LENTE").
//  3. Normalizar dobles puntos y espacios múltiples.
//  4. Bucle iterativo (hasta estable):
//     a. Quitar sufijos legales (`, C.A.`, `S.A.`, `COMPAÑIA ANONIMA`...).
//     b. Quitar sufijos geográficos (`DE VENEZUELA`, `VZLA`, `TACHIRA`...).
//     c. Quitar UN prefijo descriptivo (`LABORATORIOS`, `CORPORACION`,
//        `CRIST MEDICALS - `, `DROGUERIA`, `GRUPO`, etc.).
//  5. Trim final.
//
// El bucle se repite porque algunos nombres tienen capas anidadas
// (ej: "CRIST MEDICALS - LABORATORIO X, C.A." → "X").
var (
	parenContentRe   = regexp.MustCompile(`\(([^)]+)\)`)
	multipleSpacesRe = regexp.MustCompile(`\s+`)
	multipleDotsRe   = regexp.MustCompile(`\.{2,}`)

	// Sufijos legales: aceptan `.` o `,` como separador interno (errores de Profit).
	// `[,\s]+` exige al menos un separador antes para no comer letras de palabras
	// como "COMPOMEDICA" → "COMPOMEDI".
	legalSuffixes = []*regexp.Regexp{
		regexp.MustCompile(`(?i)[,\s]+COMPA[ÑN]IA\s+ANON[IY]?[MN]A[.,]?\s*$`),
		regexp.MustCompile(`(?i)[,\s]+S[.,]?\s*A[.,]?\s*V[.,]?\s*$`),
		regexp.MustCompile(`(?i)[,\s]+C[.,]?\s*A[.,]?\s*$`),
		regexp.MustCompile(`(?i)[,\s]+S[.,]?\s*A[.,]?\s*$`),
		regexp.MustCompile(`(?i)[,\s]+LTDA[.,]?\s*$`),
		regexp.MustCompile(`(?i)[,\s]+F[.,]?\s*P[.,]?\s*$`),
		regexp.MustCompile(`(?i)[,\s]+INC[.,]?\s*$`),
		regexp.MustCompile(`(?i)[,\s]+LLC[.,]?\s*$`),
	}

	// Sufijos geográficos a recortar después del nombre comercial.
	geoSuffixes = []*regexp.Regexp{
		regexp.MustCompile(`(?i)\s+DE\s+VENEZUELA\s*$`),
		regexp.MustCompile(`(?i)\s+DE\s+VZLA\s*$`),
		regexp.MustCompile(`(?i)\s+VENEZUELA\s*$`),
		regexp.MustCompile(`(?i)\s+VZLA\s*$`),
		regexp.MustCompile(`(?i)\s+TACHIRA\s*$`),
		regexp.MustCompile(`(?i)\s+SAN\s+CRISTOBAL\s*$`),
	}

	// Prefijos descriptivos (uno por iteración). Orden: el más específico primero
	// (CRIST MEDICALS antes que LABORATORIO, etc.).
	descriptivePrefixes = []*regexp.Regexp{
		// CRIST MEDICALS / CRISTMEDICALS / CRIST MEEDICALS (con o sin guión)
		regexp.MustCompile(`(?i)^CRIST\s*ME+DICALS\s*-?\s*`),
		regexp.MustCompile(`(?i)^CASA\s+DE\s+REPRESENTACI[OÓ]N(?:ES)?\s+`),
		regexp.MustCompile(`(?i)^COMPA[ÑN]IA\s+ANON[IY]?[MN]A\s+`),
		regexp.MustCompile(`(?i)^FABRICA\s+(?:DE\s+)?`),
		regexp.MustCompile(`(?i)^SERVICIOS\s+(?:EN\s+)?`),
		regexp.MustCompile(`(?i)^LABORATORIOS?\s+`),
		regexp.MustCompile(`(?i)^CORPORACI[OÓ]N\s+`),
		regexp.MustCompile(`(?i)^REPRESENTACIONES?\s+`),
		regexp.MustCompile(`(?i)^INVERSIONES\s+`),
		regexp.MustCompile(`(?i)^DISTRIBUIDORA\s+`),
		regexp.MustCompile(`(?i)^COMERCIALIZADORA\s+`),
		regexp.MustCompile(`(?i)^INDUSTRIAS?\s+`),
		regexp.MustCompile(`(?i)^DROGUERIA\s+`),
		regexp.MustCompile(`(?i)^GRUPO\s+`),
		regexp.MustCompile(`(?i)^IMPORTADORA\s+`),
		regexp.MustCompile(`(?i)^MANUFACTURERA\s+`),
		regexp.MustCompile(`(?i)^PRODUCCIONES\s+`),
		regexp.MustCompile(`(?i)^PRODUCTOS\s+`),
		regexp.MustCompile(`(?i)^IDEAS\s+`),
		regexp.MustCompile(`(?i)^ALIMENTOS\s+`),
		regexp.MustCompile(`(?i)^MEDICAMENTOS\s+`),
		regexp.MustCompile(`(?i)^EMPRENDIMIENTOS?\s+`),
		regexp.MustCompile(`(?i)^SUMINISTROS\s+`),
		regexp.MustCompile(`(?i)^C\.\s*A\.\s+`),
	}
)

// cleanProvDes normaliza el nombre comercial de un proveedor.
// Es idempotente: cleanProvDes(cleanProvDes(s)) == cleanProvDes(s).
func cleanProvDes(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}

	// 1. Quitar comillas dobles
	s = strings.ReplaceAll(s, `"`, "")

	// 2. Si hay paréntesis, usar SOLO el contenido del primer paréntesis
	if m := parenContentRe.FindStringSubmatch(s); m != nil {
		s = m[1]
	}

	// 3. Normalizar dobles puntos y espacios múltiples
	s = multipleDotsRe.ReplaceAllString(s, ".")
	s = multipleSpacesRe.ReplaceAllString(s, " ")
	s = strings.TrimSpace(s)

	// 4. Bucle iterativo: alterna sufijos/prefijos hasta que no haya más cambios.
	// Tope de 6 iteraciones para evitar bucles imprevistos.
	for i := 0; i < 6; i++ {
		before := s

		// 4a. Sufijos legales
		for _, re := range legalSuffixes {
			s = re.ReplaceAllString(s, "")
		}
		// 4b. Sufijos geográficos
		for _, re := range geoSuffixes {
			s = re.ReplaceAllString(s, "")
		}
		s = strings.TrimRight(s, " ,.")
		s = strings.TrimSpace(s)

		// 4c. UN prefijo descriptivo
		for _, re := range descriptivePrefixes {
			if re.MatchString(s) {
				s = re.ReplaceAllString(s, "")
				break
			}
		}
		s = strings.TrimSpace(s)

		if s == before {
			break
		}
	}

	// 5. Trim final
	s = strings.TrimSpace(s)
	s = strings.TrimRight(s, " ,.")
	return s
}
