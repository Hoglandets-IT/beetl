# Miscellaneous
Various functions not fitting in the other categories.

## SAM from DN
Extracts the first K-V pair of an Active Directory DN and returns the value

```yaml
- transformer: misc.sam_from_dn
  config:
    inField: dn
    outField: samaccouontname
```

## Lookup
Maps values from one column to another using a lookup dictionary.

```yaml
- transformer: misc.lookup
  config:
    inField: municipality
    outField: municipalityCode
    mapping:
      TRANÅS: "0564"
      STOCKHOLM: "0180"
      GÖTEBORG: "1480"
      MALMÖ: "1280"
    caseInsensitive: true
```

### Result

| municipality | municipalityCode |
|---|---|
| TRANÅS | 0564 |
| STOCKHOLM | 0180 |
| GÖTEBORG | 1480 |
| MALMÖ | 1280 |

## GUID from fields
Creates a deterministic UUIDv5 from one or more fields.

```yaml
- transformer: misc.guid_from_fields
  config:
    inFields:
      - municipalityCode
      - personNumber
    outField: externalId
    separator: "|"
    namespace: beetl
```

### Result

| municipalityCode | personNumber | externalId |
|---|---|---|
| 0564 | 191212121212 | 4f9f0d7d-... |