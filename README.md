# Handler – Streamlit app

Streamlit-app for analyse av handler i norske aksjer.

## Integrasjon i `prisanalyse`

Du har helt rett: `prisanalyse` må også vite **hvor** handler-appen kjører.

Hvis dere allerede har menypunktet via `handler_oslo_bors_setup.html` (slik du viste), er det ofte **ikke nødvendig å endre template-koden**. Den er allerede laget for å lese URL fra:

1. `HANDLER_OSLO_BORS_URL` (miljøvariabel), eller
2. `?app_url=...` i query string.

Det som manglet var en tydelig driftsoppskrift: start appen, eksponer URL, sett env i `prisanalyse`.

## Anbefalt oppsett

```text
prisanalyse/
  handler/
    main.py
    start_handler.py
    pages/
    analyses/
```

## Konkrete endringer i `prisanalyse`

### A) Start handler-prosessen (nytt eller eksisterende driftsskript)

Fra `prisanalyse`:

```bash
python handler/start_handler.py
```

`start_handler.py` starter Streamlit headless og med host/port som passer server-drift.

### B) Sett URL som menyen din allerede bruker

Siden din `handler_oslo_bors_setup.html` bruker `HANDLER_OSLO_BORS_URL`, sett denne i miljøet til `prisanalyse`:

```bash
HANDLER_OSLO_BORS_URL=https://din-handler-url.example.com
```

Da vil eksisterende menypunkt/route kunne sende brukeren til riktig app uten ytterligere endringer i template.

### C) (Valgfritt) Override host/port for handler-app

- `HANDLER_STREAMLIT_ADDRESS` (default: `0.0.0.0`)
- `HANDLER_STREAMLIT_PORT` (default: `8501`, eller `PORT` hvis satt)

Eksempel:

```bash
HANDLER_STREAMLIT_PORT=8510 python handler/start_handler.py
```

## Miljøvariabler for datakilder (handler)

For å gjøre flytting mellom repoer enklere brukes miljøvariabler for alle stier:

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
$env:HANDLER_OSLO_BORS_URL = "https://din-handler-url.example.com"
```
