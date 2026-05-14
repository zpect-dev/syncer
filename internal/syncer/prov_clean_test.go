package syncer

import "testing"

func TestCleanProvDes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		// === Ejemplos explícitos del usuario ===
		{"ej1 sufijo ,C.A.", "SALUD A TU ALCANCE S.T.A., C.A.", "SALUD A TU ALCANCE S.T.A"},
		{"ej2 sufijo C.A.", "ALIMENTOS NUTRACEUTICOS FITOSANA C.A.", "NUTRACEUTICOS FITOSANA"},
		{"ej3 comillas+prefijo+sufijo", `"""CORPORACION EMCETA, C.A"".`, "EMCETA"},
		{"ej4 prefijo LABORATORIOS+sufijo S.A.", "LABORATORIOS VARGAS, S.A.", "VARGAS"},

		// === Sufijos con coma en lugar de punto (typos en Profit) ===
		{"sufijo C,A (coma)", "LABORATORIO ZUZU C,A", "ZUZU"},
		{"sufijo S,A (coma)", "DROMEBAR S,A", "DROMEBAR"},
		{"sufijo ,C,A (doble coma)", "R&R COSMETICOS, C,A", "R&R COSMETICOS"},
		{"sufijo C,A con LABORATORIOS en medio", "ADIUM LABORATORIOS C,A", "ADIUM LABORATORIOS"},
		{"sufijo coma sin punto en LAB", "FINE CHEMICALS C.F.C C,A", "FINE CHEMICALS C.F.C"},

		// === Sufijos C.A sin separadores extra (caso comun) ===
		{"sufijo C.A simple", "MIMEDICA C.A", "MIMEDICA"},
		{"sufijo C.A sin punto final", "VITALESCA C.A", "VITALESCA"},
		{"sufijo ZAKIPHARMA C.A", "ZAKIPHARMA C.A", "ZAKIPHARMA"},
		{"sufijo SALUDMAX C.A.", "SALUDMAX C.A.", "SALUDMAX"},
		{"sufijo EMULVEN C.A", "EMULVEN C.A", "EMULVEN"},

		// === CRIST MEDICALS y variantes (todos quedan limpios) ===
		{"CRIST MEDICALS - X", "CRIST MEDICALS - BIOFARCO C.A", "BIOFARCO"},
		{"CRIST MEDICALS - X complejo", "CRIST MEDICALS - LABORATORIO PLUSANDEX, C.A", "PLUSANDEX"},
		{"CRISTMEDICALS sin espacio", "CRISTMEDICALS - H&M MEDICAL GROUP", "H&M MEDICAL GROUP"},
		{"CRIST MEEDICALS (typo)", "CRIST MEEDICALS - LABORATORIO BIONECTAR DE VENEZUELA, C.A.", "BIONECTAR"},
		{"CRIST MEDICALS - CR CRUZ VERDE", "CRISTMEDICALS - CR CRUZ VERDE", "CR CRUZ VERDE"},
		{"CRIST MEDICALS solo", "CRIST MEDICALS", ""},

		// === Sufijos geográficos ===
		{"DE VENEZUELA", "NESTLE VENEZUELA, S.A", "NESTLE"},
		{"DE VZLA", "NEILMED DE VZLA, C.A", "NEILMED"},
		{"DE VENEZUELA completo", "GAVENTEX DE VENEZUELA, C.A.", "GAVENTEX"},
		{"VENEZUELA simple", "BIONECTAR DE VENEZUELA", "BIONECTAR"},
		{"TACHIRA", "DROVENPLUS TACHIRA C.A", "DROVENPLUS"},
		{"SAN CRISTOBAL", "DSC DISTRIMEDICA SAN CRISTOBAL C.A", "DSC DISTRIMEDICA"},
		{"SOLUMEDS TACHIRA", "SOLUMEDS TACHIRA", "SOLUMEDS"},

		// === Paréntesis (decisión: usar contenido) ===
		{"paréntesis con alias", "TAHULY ZULAY CORREDOR MORENO (INVERSIONES TAMAR)", "TAMAR"},
		{"paréntesis simple", "DISTRIBUIDORA DE COMPONENTES MEDICOS JFR, C.A. (COMPOMEDICA)", "COMPOMEDICA"},
		{"paréntesis con nombre+sufijo", "ELVIA LISBETH ROJAS RANGEL (SUMINISTROS ROJAS Y ASOCIADOS, C.A)", "ROJAS Y ASOCIADOS"},

		// === Prefijos descriptivos (nuevos) ===
		{"prefijo DROGUERIA", "DROGUERIA ANDICAR C.A.", "ANDICAR"},
		{"prefijo GRUPO", "GRUPO LIALI, C.A", "LIALI"},
		{"prefijo IMPORTADORA", "IMPORTADORA AXEMEDICA C.A", "AXEMEDICA"},
		{"prefijo MANUFACTURERA", "MANUFACTURERA MUNDIAL FARMACEUTICA M.M.F., C.A.", "MUNDIAL FARMACEUTICA M.M.F"},
		{"prefijo FABRICA DE", "FABRICA DE ALIMENTOS DULCES FALIDU, C.A.", "DULCES FALIDU"},
		{"prefijo PRODUCCIONES", "PRODUCCIONES RODENEZA, C.A", "RODENEZA"},
		{"prefijo PRODUCTOS", "PRODUCTOS RONAVA", "RONAVA"},
		{"prefijo IDEAS", "IDEAS PROMOCIONALES, C.A", "PROMOCIONALES"},
		{"prefijo SERVICIOS EN", "SERVICIOS EN DESECHABLES 26178, C.A", "DESECHABLES 26178"},
		{"prefijo SUMINISTROS", "SUMINISTROS GENERICOS NIRVANA 3030 CA", "GENERICOS NIRVANA 3030"},
		{"prefijo EMPRENDIMIENTO", "EMPRENDIMIENTO ANGEL VARELA 5", "ANGEL VARELA 5"},

		// === Prefijos preexistentes ===
		{"prefijo CASA DE REPRESENTACION", "CASA DE REPRESENTACION CRIST MEDICALS, C.A", ""},
		{"prefijo CASA DE REPRESENTACIONES", "CASA DE REPRESENTACIONES R&R EQUIPOS MEDICOS, C.A.", "R&R EQUIPOS MEDICOS"},
		{"prefijo REPRESENTACIONES", "REPRESENTACIONES ISOGER C.A", "ISOGER"},
		{"prefijo INVERSIONES", "INVERSIONES MEDICAS BE 9549, C.A", "MEDICAS BE 9549"},
		{"prefijo DISTRIBUIDORA", "DISTRIBUIDORA MURAT, C.A", "MURAT"},
		{"prefijo COMERCIALIZADORA", "COMERCIALIZADORA NEOPHARMA DE VENEZUELA, C.A.", "NEOPHARMA"},
		{"prefijo INDUSTRIAS", "INDUSTRIAS PAÑAL EXPRESS C.A", "PAÑAL EXPRESS"},

		// === Compuestos: requieren iteración ===
		{"CASA DE REP + LABORATORIO", "CASA DE REPRESENTACION LABORATORIO X, C.A", "X"},
		{"C.A. prefijo (al inicio)", "C.A. PRODUCTOS RONAVA", "RONAVA"},

		// === Sufijo COMPAÑIA ANONIMA ===
		{"sufijo COMPAÑIA ANONIMA", "LABORATORIO D'EMPAIRE, COMPAÑIA ANONIMA", "D'EMPAIRE"},
		{"sufijo COMPAÑIA ANONINA (typo)", "MEDCARGO, COMPAÑIA ANONINA.", "MEDCARGO"},

		// === Otros sufijos legales ===
		{"sufijo S.A.V", "LABORATORIOS LETI, S.A.V", "LETI"},
		{"sufijo F.P", "CORPO BECCOCI, F.P", "CORPO BECCOCI"},
		{"sufijo LLC", "ZAKI GROUP LLC, C.A.", "ZAKI GROUP"},

		// === Limpieza de caracteres ===
		{"doble punto y sufijo", "BIOFARCO CHEMYCAL'S, C.A..", "BIOFARCO CHEMYCAL'S"},

		// === No tocar (sin patrones aplicables) ===
		{"sin prefijo ni sufijo", "PHARMATECH", "PHARMATECH"},
		{"nombre simple ya limpio", "EMCETA", "EMCETA"},
		{"con número y guión", "DEC MEDICA 11", "DEC MEDICA 11"},

		// === Idempotencia (sobre nombres ya limpios) ===
		{"idempotente VARGAS", "VARGAS", "VARGAS"},
		{"idempotente EMCETA", "EMCETA", "EMCETA"},

		// === Defensivos ===
		{"string vacío", "", ""},
		{"solo espacios", "   ", ""},
		{"solo comillas", `""""`, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cleanProvDes(tt.in)
			if got != tt.want {
				t.Errorf("cleanProvDes(%q)\n  got:  %q\n  want: %q", tt.in, got, tt.want)
			}
			// Verificar idempotencia
			if again := cleanProvDes(got); again != got {
				t.Errorf("no idempotente: cleanProvDes(%q) = %q vs %q", got, again, got)
			}
		})
	}
}
