package syncer

import "testing"

func TestCleanProvDes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		// Ejemplos explícitos del usuario:
		{"ej1 sufijo ,C.A.", "SALUD A TU ALCANCE S.T.A., C.A.", "SALUD A TU ALCANCE S.T.A"},
		{"ej2 sufijo C.A.", "ALIMENTOS NUTRACEUTICOS FITOSANA C.A.", "ALIMENTOS NUTRACEUTICOS FITOSANA"},
		{"ej3 comillas+prefijo+sufijo", `"""CORPORACION EMCETA, C.A"".`, "EMCETA"},
		{"ej4 prefijo LABORATORIOS+sufijo S.A.", "LABORATORIOS VARGAS, S.A.", "VARGAS"},

		// Edge cases del listado real:
		{"paréntesis con alias", "TAHULY ZULAY CORREDOR MORENO (INVERSIONES TAMAR)", "TAMAR"},
		{"paréntesis simple", "DISTRIBUIDORA DE COMPONENTES MEDICOS JFR, C.A. (COMPOMEDICA)", "COMPOMEDICA"},
		{"paréntesis con nombre+sufijo", "ELVIA LISBETH ROJAS RANGEL (SUMINISTROS ROJAS Y ASOCIADOS, C.A)", "SUMINISTROS ROJAS Y ASOCIADOS"},
		{"sufijo COMPAÑIA ANONIMA", "LABORATORIO D'EMPAIRE, COMPAÑIA ANONIMA", "D'EMPAIRE"},
		{"sufijo COMPAÑIA ANONINA (typo)", "MEDCARGO, COMPAÑIA ANONINA.", "MEDCARGO"},
		{"doble punto y sufijo", "BIOFARCO CHEMYCAL'S, C.A..", "BIOFARCO CHEMYCAL'S"},
		{"sufijo ,S.A.V", "LABORATORIOS LETI, S.A.V", "LETI"},
		{"sufijo F.P", "CORPO BECCOCI, F.P", "CORPO BECCOCI"},
		{"sufijo INC", "ZAKI GROUP LLC, C.A.", "ZAKI GROUP LLC"},
		{"prefijo CASA DE REPRESENTACION", "CASA DE REPRESENTACION CRIST MEDICALS, C.A", "CRIST MEDICALS"},
		{"prefijo CASA DE REPRESENTACIONES", "CASA DE REPRESENTACIONES R&R EQUIPOS MEDICOS, C.A.", "R&R EQUIPOS MEDICOS"},
		{"prefijo REPRESENTACIONES", "REPRESENTACIONES ISOGER C.A", "ISOGER"},
		{"prefijo INVERSIONES", "INVERSIONES MEDICAS BE 9549, C.A", "MEDICAS BE 9549"},
		{"prefijo DISTRIBUIDORA", "DISTRIBUIDORA MURAT, C.A", "MURAT"},
		{"prefijo COMERCIALIZADORA", "COMERCIALIZADORA NEOPHARMA DE VENEZUELA, C.A.", "NEOPHARMA DE VENEZUELA"},
		{"prefijo INDUSTRIAS", "INDUSTRIAS PAÑAL EXPRESS C.A", "PAÑAL EXPRESS"},
		{"sin prefijo ni sufijo", "PHARMATECH", "PHARMATECH"},

		// Idempotencia
		{"idempotente sobre nombre limpio", "VARGAS", "VARGAS"},
		{"idempotente sobre EMCETA", "EMCETA", "EMCETA"},

		// Defensivos
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
