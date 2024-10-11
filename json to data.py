import pandas as pd

data = pd.read_json('organisations.json')
data.to_csv('orgs.csv', encoding='utf-8', index=False)


data = pd.read_csv('orgs.csv')
print(data.columns.values)

def data_to_df(table):
    df = pd.DataFrame(table)
    return table

hep_keywords = [
        "high energy physics", "particle physics", "quantum field theory", "standard model",
        "quantum chromodynamics", "QCD", "Higgs boson", "collider physics", "LHC",
        "fermions", "bosons", "quarks", "leptons", "gluons", "neutrinos",
        "supersymmetry", "dark matter", "electroweak interaction", "strong interaction",
        "weak interaction", "cosmic rays", "astroparticle physics", "gravitational waves",
        "string theory", "extra dimensions", "grand unified theory", "TeV scale", "baryogenesis",
        "neutrino oscillations", "proton decay", "CERN", "muon", "tau", "proton", "antimatter",
        "dark energy", "inflationary universe", "CP violation", "neutron stars", "black holes",
        "Hawking radiation", "quantum gravity", "Planck scale", "supergravity", "heavy ion physics",
        "lattice QCD", "non-abelian gauge theories", "gauge symmetry", "standard model anomalies", "JINR",
        "quarks", "particle", "impact", "energy", "physics", "model", "germanium", "vacuun", "collider",
        "computing", "hadron", "induction", "entropy", "lambda", "lifetime", "bio"
    ]
china_keywords = ["China", "Chinese", "Chin"]

###datadf = data_to_df(data)
###datadf.drop_duplicates(subset=['DOI'])
###data
###dataset_RW = data[['Автор (ы)', 'Ключевые слова автора', 'Ключевые слова указателя', 'Организации', 'DOI']]
###dataset_RW.to_csv('april5.csv')
###dataset_RW.to_json('aaa.json')
###dataset_RW.head()
orgdata = pd.DataFrame(columns=['Организация', 'Направления'])
def search_kw(ls1, ls2, kw1, kw2):
    dataorg = []
    datahep = []
    elem1 = ls1.split('; ')
    for el1 in kw1:
        for elist in elem1:
            if el1 in elist:
                dataorg.append(elist)
    for el2 in kw2:
        for elist in ls2:
            if el2 in elist:
                datahep.append(el2)
    return dataorg, datahep


for row in data.itertuples():
    temp1, temp2 = row[1], row[2]
    temp3 = search_kw(temp1, temp2, china_keywords, hep_keywords)

    data = {
        'Организация': ''.join(str(x) for x in temp3[0]),
        'Направления': temp3[1]
    }
    orgdata = pd.concat([orgdata, pd.DataFrame([data])], ignore_index=True)
orgdata.to_csv("Keyword_orgs.csv", index=False)