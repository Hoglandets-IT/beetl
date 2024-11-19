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