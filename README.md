# Handler – Streamlit app

Streamlit-app for analyse av handler i norske aksjer.

## Integrasjon i `prisanalyse`

Ja – denne appen kan erstatte en separat `handler_oslo_bors_setup` så lenge dere peker til samme databaser og listekatalog.

Anbefalt oppsett er å legge prosjektet i en undermappe, for eksempel:

```text
prisanalyse/
  handler/
    main.py
    pages/
    analyses/
```

Kjør appen fra `prisanalyse/handler`:

```bash
streamlit run main.py
```

## Konfigurasjon via miljøvariabler

For å gjøre flytting mellom repoer enklere brukes nå miljøvariabler for alle stier:

- `HANDLER_REMOTE_DB_FULL` (default: `I:\6_EQUITIES\Database\Eiere-Database\topchanges.db`)
- `HANDLER_REMOTE_DB_RECENT` (default: `I:\6_EQUITIES\Database\Eiere-Database\topchanges_recent_60d.db`)
- `HANDLER_LIST_DIR` (default: `I:\6_EQUITIES\Database\Eiere-Styring`)
- `HANDLER_LOCAL_WORKDIR` (default: temp-katalog)
- `HANDLER_LOCAL_DB_NAME` (default: `topchanges.db`)

Eksempel (PowerShell):

```powershell
$env:HANDLER_REMOTE_DB_FULL = "I:\6_EQUITIES\Database\Eiere-Database\topchanges.db"
$env:HANDLER_REMOTE_DB_RECENT = "I:\6_EQUITIES\Database\Eiere-Database\topchanges_recent_60d.db"
$env:HANDLER_LIST_DIR = "I:\6_EQUITIES\Database\Eiere-Styring"
```

Da kan `prisanalyse` ha egne stier uten å endre kode i handler-prosjektet.
