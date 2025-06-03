import pandas as pd
import os
import json
from sqlalchemy import Date, text, Numeric, create_engine, Table, Column, String, Integer, Float, MetaData, ForeignKey


# Charger le chemin vers le fichier de configuration
chemin_config = os.path.join(r"config", "postgres_config.json")

# Lire le contenu du fichier JSON
with open(chemin_config, "r", encoding="utf-8") as fichier:
    config = json.load(fichier)

# Construire l’URL de connexion PostgreSQL
url_connexion = (
    f"postgresql+psycopg2://{config['db_user']}:{config['db_password']}"
    f"@{config['db_host']}:{config['db_port']}/{config['db_name']}"
)

# Créer un moteur SQLAlchemy
moteur = create_engine(url_connexion)

with moteur.begin() as conn:
    # Supprimer les schémas existants
    conn.execute(text('DROP SCHEMA IF EXISTS "Ventes" CASCADE;'))
    conn.execute(text('DROP SCHEMA IF EXISTS "Achats" CASCADE;'))

    # On les recrée, en s’assurant d’en être le propriétaire
    conn.execute(text('CREATE SCHEMA IF NOT EXISTS "Achats" AUTHORIZATION amco_admin;'))
    conn.execute(text('CREATE SCHEMA IF NOT EXISTS "Ventes" AUTHORIZATION amco_admin;'))

# Tester la connexion
try:
    with moteur.connect() as connexion:
        print("Connexion réussie à la base de données PostgreSQL.")
except Exception as erreur:
    print("Échec de la connexion :", erreur)

# Filtrer les colonnes d’un DataFrame selon les colonnes d’une table SQLAlchemy
def filtrer_colonnes(df, table_sqlalchemy):
    # Récupère la liste des colonnes définies dans le modèle SQLAlchemy
    colonnes_sql = [col.name for col in table_sqlalchemy.columns]
    # Sélectionne uniquement ces colonnes dans le DataFrame
    return df.loc[:, df.columns.intersection(colonnes_sql)]


# Déclaration du schéma
metadata_ventes = MetaData(schema="Ventes")
metadata_achats = MetaData(schema="Achats")

# Création des tables_ventes

## Tables : VENTES
# Table : FAMILLE
famille_ventes = Table("FAMILLE", metadata_ventes,
    Column("FA_CODEFAMILLE", String, primary_key=True, quote=True),
    Column("FA_CENTRAL", String, quote=True),
    Column("FA_INTITULE", String, nullable=False, quote=True)
)


# Table : ARTICLES
articles_ventes = Table("ARTICLES", metadata_ventes,
    Column("AR_REF", String, primary_key=True, quote=True),
    Column("AR_DESIGN", String, quote=True),
    Column("AR_PRIXACH", Numeric(10, 2), quote=True),
    Column("FA_CODEFAMILLE", String, ForeignKey("FAMILLE.FA_CODEFAMILLE"), quote=True) # Référence à la table FAMILLE
)

# Table : COMPTET
comptet_ventes = Table("COMPTET", metadata_ventes,
    Column("CT_NUM", String, primary_key=True, quote=True),
    Column("CT_INTITULE", String, quote=True),
    Column("BT_NUM", String, quote=True),
    Column("CA_NUM", String, quote=True),
    Column("CA_NUMIFRS", String, quote=True),
    Column("CBCREATEUR", String, quote=True),
    Column("CBMODIFICATION", String, quote=True),
    Column("CBREPLICATION", String, quote=True),
    Column("CG_NUMPRINC", String, quote=True),
    Column("CODE_HYPERIX_CHEZ_LE_CLIENT", String, quote=True),
    Column("CO_NO", String, quote=True),
    Column("CT_ADRESSE", String, quote=True),
    Column("CT_APE", String, quote=True),
    Column("CT_ASSURANCE", String, quote=True),
    Column("CT_BLFACT", String, quote=True),
    Column("CT_CLASSEMENT", String, quote=True),
    Column("CT_CODEPOSTAL", String, quote=True),
    Column("CT_CODEREGION", String, quote=True),
    Column("CT_COFACE", String, quote=True),
    Column("CT_COMMENTAIRE", String, quote=True),
    Column("CT_COMPLEMENT", String, quote=True),
    Column("CT_CONTACT", String, quote=True),
    Column("CT_CONTROLENC", String, quote=True),
    Column("CT_DATECREATE", String, quote=True),
    Column("CT_DATEFERMEDEBUT", String, quote=True),
    Column("CT_DATEFERMEFIN", String,quote=True),
    Column("CT_EDI1", String, quote=True),
    Column("CT_EDI2", String, quote=True),
    Column("CT_EDI3", String, quote=True),
    Column("CT_EMAIL", String, quote=True),
    Column("CT_ENCOURS", String, quote=True),
    Column("CT_FACTURE", String, quote=True),
    Column("CT_FACTUREELEC", String, quote=True),
    Column("CT_IDENTIFIANT", String, quote=True),
    Column("CT_LANGUE", String, quote=True),
    Column("CT_LETTRAGE", String, quote=True),
    Column("CT_LIVRPARTIELLE", String, quote=True),
    Column("CT_NOTPENAL", String, quote=True),
    Column("CT_NOTRAPPEL", String, quote=True),
    Column("CT_NUMCENTRALE", String, quote=True),
    Column("CT_NUMPAYEUR", String, quote=True),
    Column("CT_PAYS", String, quote=True),
    Column("CT_PRIORITELIVR", String, quote=True),
    Column("CT_QUALITE", String, quote=True),
    Column("CT_RACCOURCI", String, quote=True),
    Column("CT_REPRESENTINT", String, quote=True),
    Column("CT_REPRESENTNIF", String, quote=True),
    Column("CT_SAUT", String, quote=True),
    Column("CT_SIRET", String, quote=True),
    Column("CT_SITE", String, quote=True),
    Column("CT_SOMMEIL", String, quote=True),
    Column("CT_STATISTIQUE01", String, quote=True),
    Column("CT_STATISTIQUE02", String, quote=True),
    Column("CT_STATISTIQUE03", String, quote=True),
    Column("CT_STATISTIQUE04", String, quote=True),
    Column("CT_STATISTIQUE05", String, quote=True),
    Column("CT_STATISTIQUE06", String, quote=True),
    Column("CT_STATISTIQUE07", String, quote=True),
    Column("CT_STATISTIQUE08", String, quote=True),
    Column("CT_STATISTIQUE09", String, quote=True),
    Column("CT_STATISTIQUE10", String, quote=True),
    Column("CT_SURVEILLANCE", String, quote=True),
    Column("CT_SVCA", String, quote=True),
    Column("CT_SVCOTATION", String, quote=True),
    Column("CT_SVDATEBILAN", String, quote=True),
    Column("CT_SVDATECREATE", String, quote=True),
    Column("CT_SVDATEINCID", String, quote=True),
    Column("CT_SVDATEMAJ", String, quote=True),
    Column("CT_SVEFFECTIF", String, quote=True),
    Column("CT_SVFORMEJURI", String, quote=True),
    Column("CT_SVINCIDENT", String, quote=True),
    Column("CT_SVNBMOISBILAN", String, quote=True),
    Column("CT_SVOBJETMAJ", String, quote=True),
    Column("CT_SVPRIVIL", String, quote=True),
    Column("CT_SVREGUL", String, quote=True),
    Column("CT_SVRESULTAT", String, quote=True),
    Column("CT_TAUX01", String, quote=True),
    Column("CT_TAUX02", String, quote=True),
    Column("CT_TAUX03", String, quote=True),
    Column("CT_TAUX04", String, quote=True),
    Column("CT_TELECOPIE", String, quote=True),
    Column("CT_TELEPHONE", String, quote=True),
    Column("CT_TYPE", String, quote=True),
    Column("CT_TYPENIF", String, quote=True),
    Column("CT_VALIDECH", String, quote=True),
    Column("CT_VILLE", String, quote=True),
    Column("DE_NO", String, quote=True),
    Column("EB_NO", String, quote=True),
    Column("INT_ANALYTIQUE", String, quote=True),
    Column("INT_CATCOMPTA", String, quote=True),
    Column("INT_CATTARIF", String, quote=True),
    Column("INT_CONDITION", String, quote=True),
    Column("INT_DEVISE", String, quote=True),
    Column("INT_EXPEDITION", String, quote=True),
    Column("INT_PERIOD", String, quote=True),
    Column("INT_RISQUE", String, quote=True),
    Column("MR_NO", String, quote=True),
    Column("N_ANALYTIQUE", String, quote=True),
    Column("N_ANALYTIQUEIFRS", String, quote=True),
    Column("N_CATCOMPTA", String, quote=True),
    Column("N_CATTARIF", String, quote=True),
    Column("N_CONDITION", String, quote=True),
    Column("N_DEVISE", String, quote=True),
    Column("N_EXPEDITION", String, quote=True),
    Column("N_PERIOD", String, quote=True),
    Column("N_RISQUE", String, quote=True)
)


# Table : DOCLIGNE
docligne_ventes = Table("DOCLIGNE", metadata_ventes,
  Column("DL_NO", String, primary_key=True, quote=True),
    Column("AC_REFCLIENT", String, quote=True),
    Column("AG_NO1", String, quote=True),
    Column("AG_NO2", String, quote=True),
    Column("AR_REF", String, ForeignKey("ARTICLES.AR_REF"), quote=True),
    Column("AR_REFCOMPOSE", String, quote=True),
    Column("CA_NUM", String, quote=True),
    Column("CO_NO", String, quote=True),
    Column("CT_NUM", String, ForeignKey("COMPTET.CT_NUM") ,quote=True),
    Column("DE_NO", String, quote=True),
    Column("DLC", String, quote=True),
    Column("DLD", String, quote=True),
    Column("DL_CMUP", Numeric(10,2), quote=True),
    Column("DL_DATEAVANCEMENT", String, quote=True),
    Column("DL_DATEBC", Date, quote=True),
    Column("DL_DATEBL", Date, quote=True),
    Column("DL_DATEPL", String, quote=True),
    Column("DL_DESIGN", String, quote=True),
    Column("DL_ESCOMPTE", String, quote=True),
    Column("DL_FACTPOIDS", String, quote=True),
    Column("DL_FRAIS", String, quote=True),
    Column("DL_LIGNE", String, quote=True),
    Column("DL_MONTANTHT", Numeric(10,2), quote=True),
    Column("DL_MONTANTTTC", Numeric(10,2), quote=True),
    Column("DL_MVTSTOCK", String, quote=True),
    Column("DL_NOCOLIS", String, quote=True),
    Column("DL_NOLINK", String, quote=True),
    Column("DL_NONLIVRE", String, quote=True),
    Column("DL_NOREF", String, quote=True),
    Column("DL_PIECEBC", String, quote=True),
    Column("DL_PIECEBL", String, quote=True),
    Column("DL_PIECEPL", String, quote=True),
    Column("DL_POIDSBRUT", Numeric(10,2), quote=True),
    Column("DL_POIDSNET", Numeric(10,2), quote=True),
    Column("DL_PRIXRU", Numeric(10,2), quote=True),
    Column("DL_PRIXUNITAIRE", Numeric(10,2), quote=True),
    Column("DL_PUBC", String, quote=True),
    Column("DL_PUDEVISE", Numeric(10,2), quote=True),
    Column("DL_PUTTC", Numeric(10,2), quote=True),
    Column("DL_QTE", Numeric(10,2), quote=True),
    Column("DL_QTEBC", Numeric(10,2), quote=True),
    Column("DL_QTEBL", Numeric(10,2), quote=True),
    Column("DL_QTEPL", Numeric(10,2), quote=True),
    Column("DL_QTERESSOURCE", Numeric(10,2), quote=True),
    Column("DL_REMISE01REM_TYPE", String, quote=True),
    Column("DL_REMISE01REM_VALEUR", Numeric(10,2), quote=True),
    Column("DL_REMISE02REM_TYPE", String, quote=True),
    Column("DL_REMISE02REM_VALEUR", Numeric(10,2), quote=True),
    Column("DL_REMISE03REM_TYPE", String, quote=True),
    Column("DL_REMISE03REM_VALEUR", Numeric(10,2), quote=True),
    Column("DL_TAXE1", Numeric(10,2), quote=True),
    Column("DL_TAXE2", Numeric(10,2), quote=True),
    Column("DL_TAXE3", Numeric(10,2), quote=True),
    Column("DL_TNOMENCL", String, quote=True),
    Column("DL_TREMEXEP", String, quote=True),
    Column("DL_TREMPIED", String, quote=True),
    Column("DL_TTC", Numeric(10,2), quote=True),
    Column("DL_TYPEPL", String, quote=True),
    Column("DL_TYPETAUX1", String, quote=True),
    Column("DL_TYPETAUX2", String, quote=True),
    Column("DL_TYPETAUX3", String, quote=True),
    Column("DL_TYPETAXE1", String, quote=True),
    Column("DL_TYPETAXE2", String, quote=True),
    Column("DL_TYPETAXE3", String, quote=True),
    Column("DL_VALORISE", String, quote=True),

    Column("DO_DATE", Date, quote=True),
    Column("DO_DATELIVR", Date, quote=True),
    Column("DO_DOMAINE", String, quote=True),
    Column("DO_PIECE", String, quote=True),
    Column("DO_REF", String, quote=True),
    Column("DO_TYPE", String, quote=True),

    Column("DT_NO", String, quote=True),
    Column("EU_ENUMERE", String, quote=True),
    Column("EU_QTE", Numeric(10,2), quote=True),

    Column("FNT_MONTANTHT", Numeric(10,2), quote=True),
    Column("FNT_MONTANTHTSIGNE", Numeric(10,2), quote=True),
    Column("FNT_MONTANTTAXES", Numeric(10,2), quote=True),
    Column("FNT_MONTANTTTC", Numeric(10,2), quote=True),
    Column("FNT_MONTANTTTCSIGNE", Numeric(10,2), quote=True),
    Column("FNT_PRIXUNET", Numeric(10,2), quote=True),
    Column("FNT_PRIXUNETDEVISE", Numeric(10,2), quote=True),
    Column("FNT_PRIXUNETTTC", Numeric(10,2), quote=True),
    Column("FNT_QTESIGNE", Numeric(10,2), quote=True),
    Column("FNT_REMISEGLOBALE", Numeric(10,2), quote=True),

    Column("LS_COMPLEMENT", String, quote=True),
    Column("LS_FABRICATION", String, quote=True),
    Column("LS_NOSERIE", String, quote=True),
    Column("LS_PEREMPTION", String, quote=True),

    Column("N°_DE_LOT/_CURE_DATE", String, quote=True),
    Column("QTE_ACCESS", Numeric(10,2), quote=True),
    Column("RP_CODE", String, quote=True),
)

## Tables : ACHATS

# Table : FAMILLE
famille_achats = Table("FAMILLE", metadata_achats,
    Column("FA_CODEFAMILLE", String, primary_key=True, quote=True),
    Column("FA_CENTRAL", String, quote=True),
    Column("FA_INTITULE", String, nullable=False, quote=True)
)

# Table : ARTICLES
articles_achats = Table("ARTICLES", metadata_achats,
    Column("AR_REF", String, primary_key=True, quote=True),
    Column("AR_DESIGN", String, quote=True),
    Column("AR_PRIXACH", Numeric(10, 2), quote=True),
    Column("FA_CODEFAMILLE", String, ForeignKey("FAMILLE.FA_CODEFAMILLE"), quote=True) # Référence à la table FAMILLE
)
# Table : COMPTET
comptet_achats = Table("COMPTET", metadata_achats,
    Column("CT_NUM", String, primary_key=True, quote=True),
    Column("CT_INTITULE", String, quote=True),
    Column("BT_NUM", String, quote=True),
    Column("CA_NUM", String, quote=True),
    Column("CA_NUMIFRS", String, quote=True),
    Column("CBCREATEUR", String, quote=True),
    Column("CBMODIFICATION", String, quote=True),
    Column("CBREPLICATION", String, quote=True),
    Column("CG_NUMPRINC", String, quote=True),
    Column("CODE_HYPERIX_CHEZ_LE_CLIENT", String, quote=True),
    Column("CO_NO", String, quote=True),
    Column("CT_ADRESSE", String, quote=True),
    Column("CT_APE", String, quote=True),
    Column("CT_ASSURANCE", String, quote=True),
    Column("CT_BLFACT", String, quote=True),
    Column("CT_CLASSEMENT", String, quote=True),
    Column("CT_CODEPOSTAL", String, quote=True),
    Column("CT_CODEREGION", String, quote=True),
    Column("CT_COFACE", String, quote=True),
    Column("CT_COMMENTAIRE", String, quote=True),
    Column("CT_COMPLEMENT", String, quote=True),
    Column("CT_CONTACT", String, quote=True),
    Column("CT_CONTROLENC", String, quote=True),
    Column("CT_DATECREATE", String, quote=True),
    Column("CT_DATEFERMEDEBUT", String, quote=True),
    Column("CT_DATEFERMEFIN", String,quote=True),
    Column("CT_EDI1", String, quote=True),
    Column("CT_EDI2", String, quote=True),
    Column("CT_EDI3", String, quote=True),
    Column("CT_EMAIL", String, quote=True),
    Column("CT_ENCOURS", String, quote=True),
    Column("CT_FACTURE", String, quote=True),
    Column("CT_FACTUREELEC", String, quote=True),
    Column("CT_IDENTIFIANT", String, quote=True),
    Column("CT_LANGUE", String, quote=True),
    Column("CT_LETTRAGE", String, quote=True),
    Column("CT_LIVRPARTIELLE", String, quote=True),
    Column("CT_NOTPENAL", String, quote=True),
    Column("CT_NOTRAPPEL", String, quote=True),
    Column("CT_NUMCENTRALE", String, quote=True),
    Column("CT_NUMPAYEUR", String, quote=True),
    Column("CT_PAYS", String, quote=True),
    Column("CT_PRIORITELIVR", String, quote=True),
    Column("CT_QUALITE", String, quote=True),
    Column("CT_RACCOURCI", String, quote=True),
    Column("CT_REPRESENTINT", String, quote=True),
    Column("CT_REPRESENTNIF", String, quote=True),
    Column("CT_SAUT", String, quote=True),
    Column("CT_SIRET", String, quote=True),
    Column("CT_SITE", String, quote=True),
    Column("CT_SOMMEIL", String, quote=True),
    Column("CT_STATISTIQUE01", String, quote=True),
    Column("CT_STATISTIQUE02", String, quote=True),
    Column("CT_STATISTIQUE03", String, quote=True),
    Column("CT_STATISTIQUE04", String, quote=True),
    Column("CT_STATISTIQUE05", String, quote=True),
    Column("CT_STATISTIQUE06", String, quote=True),
    Column("CT_STATISTIQUE07", String, quote=True),
    Column("CT_STATISTIQUE08", String, quote=True),
    Column("CT_STATISTIQUE09", String, quote=True),
    Column("CT_STATISTIQUE10", String, quote=True),
    Column("CT_SURVEILLANCE", String, quote=True),
    Column("CT_SVCA", String, quote=True),
    Column("CT_SVCOTATION", String, quote=True),
    Column("CT_SVDATEBILAN", String, quote=True),
    Column("CT_SVDATECREATE", String, quote=True),
    Column("CT_SVDATEINCID", String, quote=True),
    Column("CT_SVDATEMAJ", String, quote=True),
    Column("CT_SVEFFECTIF", String, quote=True),
    Column("CT_SVFORMEJURI", String, quote=True),
    Column("CT_SVINCIDENT", String, quote=True),
    Column("CT_SVNBMOISBILAN", String, quote=True),
    Column("CT_SVOBJETMAJ", String, quote=True),
    Column("CT_SVPRIVIL", String, quote=True),
    Column("CT_SVREGUL", String, quote=True),
    Column("CT_SVRESULTAT", String, quote=True),
    Column("CT_TAUX01", String, quote=True),
    Column("CT_TAUX02", String, quote=True),
    Column("CT_TAUX03", String, quote=True),
    Column("CT_TAUX04", String, quote=True),
    Column("CT_TELECOPIE", String, quote=True),
    Column("CT_TELEPHONE", String, quote=True),
    Column("CT_TYPE", String, quote=True),
    Column("CT_TYPENIF", String, quote=True),
    Column("CT_VALIDECH", String, quote=True),
    Column("CT_VILLE", String, quote=True),
    Column("DE_NO", String, quote=True),
    Column("EB_NO", String, quote=True),
    Column("INT_ANALYTIQUE", String, quote=True),
    Column("INT_CATCOMPTA", String, quote=True),
    Column("INT_CATTARIF", String, quote=True),
    Column("INT_CONDITION", String, quote=True),
    Column("INT_DEVISE", String, quote=True),
    Column("INT_EXPEDITION", String, quote=True),
    Column("INT_PERIOD", String, quote=True),
    Column("INT_RISQUE", String, quote=True),
    Column("MR_NO", String, quote=True),
    Column("N_ANALYTIQUE", String, quote=True),
    Column("N_ANALYTIQUEIFRS", String, quote=True),
    Column("N_CATCOMPTA", String, quote=True),
    Column("N_CATTARIF", String, quote=True),
    Column("N_CONDITION", String, quote=True),
    Column("N_DEVISE", String, quote=True),
    Column("N_EXPEDITION", String, quote=True),
    Column("N_PERIOD", String, quote=True),
    Column("N_RISQUE", String, quote=True)
)


# Table : F_ARTFOURNISS
fournisseur_achats = Table(
    "ARTFOURNISS",
    metadata_achats,
    Column("AF_REFFOURNISS", String, primary_key=True, quote=True),
    Column("AF_CODEBARRE", String, quote=True),
    Column("AF_COLISAGE", String, quote=True),
    Column("AF_CONVDIV", String, quote=True),
    Column("AF_CONVERSION", String, quote=True),
    Column("AF_DATEAPPLICATION", Date, quote=True),
    Column("AF_DELAIAPPRO", Integer, quote=True),
    Column("AF_DEVISE", String, quote=True),
    Column("AF_GARANTIE", String, quote=True),
    Column("AF_PRINCIPAL", String, quote=True),
    Column("AF_PRIXACH", Numeric(10, 2), quote=True),
    Column("AF_PRIXACHNOUV", Numeric(10, 2), quote=True),
    Column("AF_PRIXDEV", Numeric(10, 2), quote=True),
    Column("AF_PRIXDEVNOUV", Numeric(10, 2), quote=True),
    Column("AF_QTEMINI", Numeric(10, 2), quote=True),
    Column("AF_QTEMONT", Numeric(10, 2), quote=True),
    Column("AF_REMISE", Numeric(10, 2), quote=True),
    Column("AF_REMISENOUV", Numeric(10, 2), quote=True),
    Column("AF_TYPEREM", String, quote=True),
    Column("AF_UNITE", String, quote=True),
    Column("AR_REF", String, ForeignKey("ARTICLES.AR_REF"), quote=True),
    Column("CT_NUM", String, quote=True),
    Column("EG_CHAMP", String, quote=True),
    Column("INT_CHAMP", String, quote=True),
    Column("INT_DEVISE", String, quote=True),
    Column("INT_UNITE", String, quote=True),
    Column("CBCREATEUR", String, quote=True),
    Column("CBMODIFICATION", String, quote=True),
    Column("CBREPLICATION", String, quote=True),
)

# Table : DOCLIGNE (Achats)
docligne_achats = Table(
    "DOCLIGNE", metadata_achats,
    Column("DL_NO", String, primary_key=True, quote=True),
    Column("AC_REFCLIENT", String, quote=True),
    Column("AF_REFFOURNISS", String, ForeignKey("ARTFOURNISS.AF_REFFOURNISS"), quote=True),
    Column("AG_NO1", String, quote=True),
    Column("AG_NO2", String, quote=True),
    Column("AR_REF", String, ForeignKey("ARTICLES.AR_REF"), quote=True),
    Column("AR_REFCOMPOSE", String, quote=True),
    Column("CA_NUM", String, quote=True),
    Column("CO_NO", String, quote=True),
    Column("CT_NUM", String, ForeignKey("COMPTET.CT_NUM") ,quote=True),
    Column("DE_NO", String, quote=True),
    Column("DLC", String, quote=True),
    Column("DLD", String, quote=True),
    Column("DL_CMUP", Numeric(10,2), quote=True),
    Column("DL_DATEAVANCEMENT", String, quote=True),
    Column("DL_DATEBC", Date, quote=True),
    Column("DL_DATEBL", Date, quote=True),
    Column("DL_DATEPL", String, quote=True),
    Column("DL_DESIGN", String, quote=True),
    Column("DL_ESCOMPTE", String, quote=True),
    Column("DL_FACTPOIDS", String, quote=True),
    Column("DL_FRAIS", String, quote=True),
    Column("DL_LIGNE", String, quote=True),
    Column("DL_MONTANTHT", Numeric(10,2), quote=True),
    Column("DL_MONTANTTTC", Numeric(10,2), quote=True),
    Column("DL_MVTSTOCK", String, quote=True),
    Column("DL_NOCOLIS", String, quote=True),
    Column("DL_NOLINK", String, quote=True),
    Column("DL_NONLIVRE", String, quote=True),
    Column("DL_NOREF", String, quote=True),
    Column("DL_PIECEBC", String, quote=True),
    Column("DL_PIECEBL", String, quote=True),
    Column("DL_PIECEPL", String, quote=True),
    Column("DL_POIDSBRUT", Numeric(10,2), quote=True),
    Column("DL_POIDSNET", Numeric(10,2), quote=True),
    Column("DL_PRIXRU", Numeric(10,2), quote=True),
    Column("DL_PRIXUNITAIRE", Numeric(10,2), quote=True),
    Column("DL_PUBC", String, quote=True),
    Column("DL_PUDEVISE", Numeric(10,2), quote=True),
    Column("DL_PUTTC", Numeric(10,2), quote=True),
    Column("DL_QTE", Numeric(10,2), quote=True),
    Column("DL_QTEBC", Numeric(10,2), quote=True),
    Column("DL_QTEBL", Numeric(10,2), quote=True),
    Column("DL_QTEPL", Numeric(10,2), quote=True),
    Column("DL_QTERESSOURCE", Numeric(10,2), quote=True),
    Column("DL_REMISE01REM_TYPE", String, quote=True),
    Column("DL_REMISE01REM_VALEUR", Numeric(10,2), quote=True),
    Column("DL_REMISE02REM_TYPE", String, quote=True),
    Column("DL_REMISE02REM_VALEUR", Numeric(10,2), quote=True),
    Column("DL_REMISE03REM_TYPE", String, quote=True),
    Column("DL_REMISE03REM_VALEUR", Numeric(10,2), quote=True),
    Column("DL_TAXE1", Numeric(10,2), quote=True),
    Column("DL_TAXE2", Numeric(10,2), quote=True),
    Column("DL_TAXE3", Numeric(10,2), quote=True),
    Column("DL_TNOMENCL", String, quote=True),
    Column("DL_TREMEXEP", String, quote=True),
    Column("DL_TREMPIED", String, quote=True),
    Column("DL_TTC", Numeric(10,2), quote=True),
    Column("DL_TYPEPL", String, quote=True),
    Column("DL_TYPETAUX1", String, quote=True),
    Column("DL_TYPETAUX2", String, quote=True),
    Column("DL_TYPETAUX3", String, quote=True),
    Column("DL_TYPETAXE1", String, quote=True),
    Column("DL_TYPETAXE2", String, quote=True),
    Column("DL_TYPETAXE3", String, quote=True),
    Column("DL_VALORISE", String, quote=True),

    Column("DO_DATE", Date, quote=True),
    Column("DO_DATELIVR", Date, quote=True),
    Column("DO_DOMAINE", String, quote=True),
    Column("DO_PIECE", String, quote=True),
    Column("DO_REF", String, quote=True),
    Column("DO_TYPE", String, quote=True),

    Column("DT_NO", String, quote=True),
    Column("EU_ENUMERE", String, quote=True),
    Column("EU_QTE", Numeric(10,2), quote=True),

    Column("FNT_MONTANTHT", Numeric(10,2), quote=True),
    Column("FNT_MONTANTHTSIGNE", Numeric(10,2), quote=True),
    Column("FNT_MONTANTTAXES", Numeric(10,2), quote=True),
    Column("FNT_MONTANTTTC", Numeric(10,2), quote=True),
    Column("FNT_MONTANTTTCSIGNE", Numeric(10,2), quote=True),
    Column("FNT_PRIXUNET", Numeric(10,2), quote=True),
    Column("FNT_PRIXUNETDEVISE", Numeric(10,2), quote=True),
    Column("FNT_PRIXUNETTTC", Numeric(10,2), quote=True),
    Column("FNT_QTESIGNE", Numeric(10,2), quote=True),
    Column("FNT_REMISEGLOBALE", Numeric(10,2), quote=True),

    Column("LS_COMPLEMENT", String, quote=True),
    Column("LS_FABRICATION", String, quote=True),
    Column("LS_NOSERIE", String, quote=True),
    Column("LS_PEREMPTION", String, quote=True),

    Column("N°_DE_LOT/_CURE_DATE", String, quote=True),
    Column("QTE_ACCESS", Numeric(10,2), quote=True),
    Column("RP_CODE", String, quote=True),
)

# Fonction pour nettoyer les DataFrames
def nettoyer_dataframe(df):
    df = df.dropna(axis=1, how="all")  # Supprimer les colonnes entièrement vides
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].replace('nan', None)
    for col in df.columns:
        if "DATE" in col.upper():
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
    df = df.where(pd.notna(df), None)
    return df


#  Créer les tables dans la base de données PostgreSQL
metadata_achats.create_all(moteur, checkfirst=True)
metadata_ventes.create_all(moteur, checkfirst=True)


# Définir le chemin du dossier des fichiers_ventes
chemin_dossier = os.path.join(r"extraits", "xlsx_propres")

fichiers_ventes = {
    "famille_ventes": "F_FAMILLE_propre.xlsx",
    "articles_ventes": "F_ARTICLE_propre.xlsx",
    "comptet_ventes": "F_COMPTET_propre.xlsx",
    "docligne_ventes": "F_DOCLIGNE_propre.xlsx"
}

fichiers_achats = {
    "famille_achats": "F_FAMILLE_propre.xlsx",
    "articles_achats": "F_ARTICLE_propre.xlsx",
    "comptet_achats": "F_COMPTET_propre.xlsx",
    "fournisseur_achats": "F_ARTFOURNISS_propre.xlsx",
    "docligne_achats": "F_DOCLIGNE_propre.xlsx"
}

# Dictionnaire où seront stockés les DataFrames
tables_ventes = {}
tables_achats = {}

# Dictoinnaire où seront stockés les DataFrames des ventes
for nom_table, nom_fichier in fichiers_ventes.items():
    chemin_complet = os.path.join(chemin_dossier, nom_fichier)
    if os.path.exists(chemin_complet):
        df = pd.read_excel(chemin_complet)
        
        # Corriger les encodages malins (ex: CT_INTITULE avec accents)
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).apply(
                lambda x: x.encode('latin1', errors='ignore').decode('utf-8', errors='replace')
            )

        tables_ventes[nom_table] = df

#Dictoinnaire où seront stockés les DataFrames des achats
for nom_table, nom_fichier in fichiers_achats.items():
    chemin_complet = os.path.join(chemin_dossier, nom_fichier)
    
    if os.path.exists(chemin_complet):
        df = pd.read_excel(chemin_complet)

        # 🔧 Reparar texto mal codificado: latin1 → utf-8
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).apply(
                lambda x: x.encode('latin1', errors='ignore').decode('utf-8', errors='replace')
            )

        tables_achats[nom_table] = df
    else:
        print(f"Fichier non trouvé : {chemin_complet}")



# Filtrage automatique des colonnes pour ventes (utiliser toutes les colonnes)
for nom_table in tables_ventes:
    df = tables_ventes[nom_table]
    df_filtré = nettoyer_dataframe(df)
    tables_ventes[nom_table] = df_filtré
    print(f"Colonnes nettoyées pour {nom_table} (Ventes) : {df_filtré.columns.tolist()}")



# Filtrage automatique des colonnes pour achats (utiliser toutes les colonnes)
for nom_table in tables_achats:
    df = tables_achats[nom_table]
    df_filtré = nettoyer_dataframe(df)
    tables_achats[nom_table] = df_filtré
    print(f"Colonnes nettoyées pour {nom_table} (Achats) : {df_filtré.columns.tolist()}")



# Normaliser les clés AF_REFFOURNISS pour qu'elles correspondent exactement
# aux valeurs de la table fournisseur_achats

# 1. Récupérer la liste des clés valides (telles-quelles) depuis fournisseur_achats
fourn_vals = (
    tables_achats["fournisseur_achats"]["AF_REFFOURNISS"]
    .dropna()
    .astype(str)
    .str.strip()
    .unique()
)

# 2. Construire un masque booléen sur docligne_achats, sur la même Série
df_dl = tables_achats["docligne_achats"]

mask = (
    df_dl["AF_REFFOURNISS"].notna()  # on ne veut pas des NaN
) & (
    df_dl["AF_REFFOURNISS"].astype(str)
         .str.strip()
         .isin(fourn_vals)
)

# 3. Appliquer le filtre en conservant l’index
tables_achats["docligne_achats"] = df_dl.loc[mask]

print(f"{len(tables_achats['docligne_achats'])} lignes de DOCLIGNE conservées après filtrage.")



# Ordre d’insertion selon les dépendances entre les tables_ventes
ordre_insertion_ventes = ["famille_ventes", "articles_ventes", "comptet_ventes", "docligne_ventes"]

# Ordre d’insertion selon les dépendances entre les tables_ventes
ordre_insertion_achats = ["famille_achats", "articles_achats", "comptet_achats", "fournisseur_achats", "docligne_achats"]

# Correspondance avec les noms réels des tables_ventes PostgreSQL (en majuscules)
noms_sql_ventes = {
    "famille_ventes": "FAMILLE",
    "articles_ventes": "ARTICLES",
    "comptet_ventes": "COMPTET",
    "docligne_ventes": "DOCLIGNE"
}

# Correspondance pour achats
noms_sql_achats = {
    "famille_achats": "FAMILLE",
    "articles_achats": "ARTICLES",
    "comptet_achats": "COMPTET",
    "fournisseur_achats": "ARTFOURNISS",
    "docligne_achats": "DOCLIGNE"
}


# Insertion des données dans les tables_ventes
for nom_logique in ordre_insertion_ventes:
    nom_table_sql = noms_sql_ventes[nom_logique]
    table_obj = {
        "famille_ventes": famille_ventes,
        "articles_ventes": articles_ventes,
        "comptet_ventes": comptet_ventes,
        "docligne_ventes": docligne_ventes
    }[nom_logique]
    df = tables_ventes[nom_logique]

    # 1) Ne garder que les colonnes du modèle
    df_filtré = filtrer_colonnes(df, table_obj)

    # 2) Remplacer les points isolés par NA
    df_filtré = df_filtré.replace({".": pd.NA})

    if nom_logique == "comptet_ventes":
        df_filtré = df_filtré.dropna(subset=["CT_NUM"])
        df_filtré["CT_INTITULE"] = df_filtré["CT_INTITULE"].astype(str)

    elif nom_logique == "docligne_ventes":
        df_filtré = df_filtré.dropna(subset=["DL_NO", "AR_REF"])
        # Nettoyage et filtrage des clés orphelines
        df_filtré["CT_NUM"] = (
            df_filtré["CT_NUM"]
            .astype(str)
            .str.strip()
            .str.replace(r"\.0$", "", regex=True)
        )


        # Nettoyage des colonnes de type chaîne de caractères
        df_filtré["CT_NUM"]       = df_filtré["CT_NUM"].astype(str)
        df_filtré["AC_REFCLIENT"] = df_filtré["AC_REFCLIENT"].astype(str)
        df_filtré["AR_REF"]       = df_filtré["AR_REF"].astype(str)
        df_filtré["DL_DESIGN"]    = df_filtré["DL_DESIGN"].astype(str)

        # Conversion des colonnes numériques
        df_filtré["DL_QTE"]           = pd.to_numeric(df_filtré["DL_QTE"],           errors="coerce")
        df_filtré["DL_PRIXUNITAIRE"]  = pd.to_numeric(df_filtré["DL_PRIXUNITAIRE"],  errors="coerce")
        df_filtré["DL_MONTANTHT"]     = pd.to_numeric(df_filtré["DL_MONTANTHT"],     errors="coerce")

        ct_comptet = set(tables_ventes["comptet_ventes"]["CT_NUM"]
                         .astype(str).str.strip())
        df_filtré = df_filtré[df_filtré["CT_NUM"].isin(ct_comptet)]

    # 3) Supprimer les lignes sans clé primaire
    elif nom_logique == "articles_ventes":
        # Supprimer les lignes sans clé primaire AR_REF
        df_filtré = df_filtré.dropna(subset=["AR_REF"])

        # Nettoyage des colonnes de type chaîne de caractères
        df_filtré["AR_REF"] = df_filtré["AR_REF"].astype(str)

    elif nom_logique == "famille_ventes":
        # Supprimer les lignes sans clé primaire FA_CODEFAMILLE
        df_filtré["FA_CENTRAL"]  = df_filtré["FA_CENTRAL"].astype(str)
        df_filtré["FA_INTITULE"] = df_filtré["FA_INTITULE"].astype(str)

    # 4) Insertion
    df_filtré.to_sql(
        nom_table_sql,
        moteur,
        schema="Ventes",
        if_exists="append",
        index=False
    )

    print(f"Inséré dans Ventes.{nom_table_sql} cols : {df_filtré.columns.tolist()}")

print("Données des ventes insérées avec succès.")

# Insertion des données dans les tables_achats
for nom_logique in ordre_insertion_achats:
    nom_table_sql = noms_sql_achats[nom_logique]
    table_obj = {
        "famille_achats":   famille_achats,
        "articles_achats":  articles_achats,
        "comptet_achats":   comptet_achats,
        "fournisseur_achats": fournisseur_achats,
        "docligne_achats":  docligne_achats
    }[nom_logique]
    df = tables_achats[nom_logique]

    # 1) Ne garder que les colonnes du modèle
    df_filtré = filtrer_colonnes(df, table_obj)

    # 2) Remplacer les points isolés par NA
    df_filtré = df_filtré.replace({".": pd.NA})

    # 1) On nettoie COMPTET en premier
    if nom_logique == "comptet_achats":
        # Supprimer les lignes sans clé primaire CT_NUM
        df_filtré = df_filtré.dropna(subset=["CT_NUM"])
        # Nettoyage des colonnes de type chaîne de caractères
        df_filtré["CT_NUM"]      = df_filtré["CT_NUM"].astype(str)
        df_filtré["CT_INTITULE"] = df_filtré["CT_INTITULE"].astype(str)

    elif nom_logique == "docligne_achats":
        df_filtré = df_filtré.dropna(subset=["DL_NO", "AF_REFFOURNISS", "AR_REF"])
        # Nettoyage et filtrage des clés orphelines
        df_filtré["CT_NUM"] = (
            df_filtré["CT_NUM"]
            .astype(str)
            .str.strip()
            .str.replace(r"\.0$", "", regex=True)
        )

        # Nettoyage des colonnes de type chaîne de caractères
        df_filtré["CT_NUM"]       = df_filtré["CT_NUM"].astype(str)
        df_filtré["AC_REFCLIENT"] = df_filtré["AC_REFCLIENT"].astype(str)
        df_filtré["AR_REF"]       = df_filtré["AR_REF"].astype(str)
        df_filtré["DL_DESIGN"]    = df_filtré["DL_DESIGN"].astype(str)

        # Conversion des colonnes numériques
        df_filtré["DL_QTE"]           = pd.to_numeric(df_filtré["DL_QTE"],           errors="coerce")
        df_filtré["DL_PRIXUNITAIRE"]  = pd.to_numeric(df_filtré["DL_PRIXUNITAIRE"],  errors="coerce")
        df_filtré["DL_MONTANTHT"]     = pd.to_numeric(df_filtré["DL_MONTANTHT"],     errors="coerce")

        ct_comptet = set(tables_achats["comptet_achats"]["CT_NUM"]
                         .astype(str).str.strip())
        df_filtré = df_filtré[df_filtré["CT_NUM"].isin(ct_comptet)]

    elif nom_logique == "fournisseur_achats":
        # 3a) Supprimer les lignes sans clé primaire AF_REFFOURNISS
        df_filtré = df_filtré.dropna(subset=["AF_REFFOURNISS"])
        # 3b) Supprimer les doublons
        df_filtré = df_filtré.drop_duplicates(subset=["AF_REFFOURNISS"])
        # Nettoyage des colonnes de type chaîne de caractères
        df_filtré["AF_REFFOURNISS"] = df_filtré["AF_REFFOURNISS"].astype(str)


    elif nom_logique == "articles_achats":
        # Supprimer les lignes sans clé primaire AR_REF
        df_filtré = df_filtré.dropna(subset=["AR_REF"])

        # Nettoyage des colonnes de type chaîne de caractères
        df_filtré["AR_REF"] = df_filtré["AR_REF"].astype(str)


    elif nom_logique == "famille_achats":
        # Supprimer les lignes sans clé primaire FA_CODEFAMILLE
        df_filtré["FA_CENTRAL"]  = df_filtré["FA_CENTRAL"].astype(str)
        df_filtré["FA_INTITULE"] = df_filtré["FA_INTITULE"].astype(str)

    # 4) Insertion
    df_filtré.to_sql(
        nom_table_sql,
        moteur,
        schema="Achats",
        if_exists="append",
        index=False
    )
    print(f"Inséré dans Achats.{nom_table_sql}, {len(df_filtré)} lignes.")

print("Données des achats insérées avec succès.")

# Fermer le moteur proprement
moteur.dispose()
print("Connexion fermée proprement.")

