package syncer

import (
	"regexp"
	"strings"
)

// Reglas de limpieza de `prov_des`. Se aplican en este orden:
//  1. Quitar comillas dobles.
//  2. Si hay paréntesis, usar SOLO el contenido del primer paréntesis
//     (ej: "JAIME GOMEZ (EL SURTIDOR DEL LENTE)" -> "EL SURTIDOR DEL LENTE").
//  3. Normalizar caracteres redundantes (dobles puntos, espacios múltiples).
//  4. Eliminar sufijos legales (`, C.A.`, ` S.A.`, `COMPAÑIA ANONIMA`, etc.).
//  5. Eliminar prefijo descriptivo (`LABORATORIOS`, `CORPORACION`, ...).
//  6. Trim final de comas, puntos y espacios.
var (
	parenContentRe   = regexp.MustCompile(`\(([^)]+)\)`)
	multipleSpacesRe = regexp.MustCompile(`\s+`)
	multipleDotsRe   = regexp.MustCompile(`\.{2,}`)

	// Sufijos legales en orden de prioridad (más específicos primero).
	// (?i) = case-insensitive. Requerimos `[,\s]+` (al menos un separador)
	// para evitar comer letras finales de palabras como "COMPOMEDICA" -> "COMPOMEDI".
	legalSuffixes = []*regexp.Regexp{
		regexp.MustCompile(`(?i)[,\s]+COMPA[ÑN]IA\s+ANON[IY]?[MN]A\.?\s*$`),
		regexp.MustCompile(`(?i)[,\s]+S\.?\s*A\.?\s*V\.?\s*$`),
		regexp.MustCompile(`(?i)[,\s]+C\.?\s*A\.?\s*$`),
		regexp.MustCompile(`(?i)[,\s]+S\.?\s*A\.?\s*$`),
		regexp.MustCompile(`(?i)[,\s]+LTDA\.?\s*$`),
		regexp.MustCompile(`(?i)[,\s]+F\.P\.?\s*$`),
		regexp.MustCompile(`(?i)[,\s]+INC\.?\s*$`),
	}

	// Prefijos descriptivos (solo se elimina UNO por iteración, para no perder palabras esenciales).
	descriptivePrefixes = []*regexp.Regexp{
		regexp.MustCompile(`(?i)^LABORATORIOS?\s+`),
		regexp.MustCompile(`(?i)^CORPORACI[OÓ]N\s+`),
		regexp.MustCompile(`(?i)^CASA\s+DE\s+REPRESENTACI[OÓ]N(?:ES)?\s+`),
		regexp.MustCompile(`(?i)^REPRESENTACIONES?\s+`),
		regexp.MustCompile(`(?i)^INVERSIONES\s+`),
		regexp.MustCompile(`(?i)^DISTRIBUIDORA\s+`),
		regexp.MustCompile(`(?i)^COMERCIALIZADORA\s+`),
		regexp.MustCompile(`(?i)^INDUSTRIAS?\s+`),
		regexp.MustCompile(`(?i)^COMPA[ÑN]IA\s+ANON[IY]?[MN]A\s+`),
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

	// 2. Si hay paréntesis, usar SOLO el contenido (decisión del usuario)
	if m := parenContentRe.FindStringSubmatch(s); m != nil {
		s = m[1]
	}

	// 3. Normalizar dobles puntos y espacios múltiples
	s = multipleDotsRe.ReplaceAllString(s, ".")
	s = multipleSpacesRe.ReplaceAllString(s, " ")
	s = strings.TrimSpace(s)

	// 4. Sufijos: iterar mientras haya coincidencias (encadenamientos como "S.A., C.A.")
	for {
		before := s
		for _, re := range legalSuffixes {
			s = re.ReplaceAllString(s, "")
		}
		s = strings.TrimRight(s, " ,.")
		s = strings.TrimSpace(s)
		if s == before {
			break
		}
	}

	// 5. Prefijos descriptivos: SOLO uno (evitar destruir nombres como "INDUSTRIAS LABORATORIO X")
	for _, re := range descriptivePrefixes {
		if re.MatchString(s) {
			s = re.ReplaceAllString(s, "")
			break
		}
	}

	// 6. Trim final
	s = strings.TrimSpace(s)
	s = strings.TrimRight(s, " ,.")
	return s
}
