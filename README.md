## Dependancies
```
python3 -m pip install co-connect-tools
```

## Example Synthetic Data

* part1: 100k people with person ids from pk1-pk100000 
* part2: 40k people with person ids from pk100001-pk140000
* part3: 30k people with persons defined in demographics from part1 & part2
   * no demographics, Hospital Visits, Vaccinations

```
data/
├── part1
│   ├── Blood_Test.csv
│   ├── Demographics.csv
│   ├── GP_Records.csv
│   ├── Hospital_Visit.csv
│   ├── Serology.csv
│   ├── Symptoms.csv
│   └── Vaccinations.csv
├── part2
│   ├── Blood_Test.csv
│   ├── Demographics.csv
│   ├── GP_Records.csv
│   ├── Hospital_Visit.csv
│   ├── Serology.csv
│   ├── Symptoms.csv
│   └── Vaccinations.csv
└── part3
    ├── Blood_Test.csv
    ├── Hospital_Visit.csv
    ├── Serology.csv
    └── Symptoms.csv

3 directories, 18 files
```

## Generating Synthetic Data

Simply run:
```
python generate.py
```


