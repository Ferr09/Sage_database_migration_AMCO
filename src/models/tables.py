# -- coding: utf-8 --

from sqlalchemy import (
    MetaData, Table, Column, String, Numeric, Integer, ForeignKey, TIMESTAMP
)

# --------------------------------------------------------------------
# Déclaration des métadonnées SANS information de schéma
# --------------------------------------------------------------------
metadata_ventes = MetaData()
metadata_achats = MetaData()

# --------------------------------------------------------------------
# Création des tables Ventes
# Toutes les colonnes qui ne sont pas dans les tables générales sont définies comme String.
# --------------------------------------------------------------------

famille_ventes = Table(
    "FAMILLE", metadata_ventes,
    # Préservé : dans la table générale
    Column("FA_CODEFAMILLE", String(50), primary_key=True, quote=True),
    # Préservé : dans la table générale
    Column("FA_CENTRAL",   String(50), quote=True),
    # Préservé : dans la table générale
    Column("FA_INTITULE",  String(255), nullable=False, quote=True)
)

articles_ventes = Table(
    "ARTICLES", metadata_ventes,
    # Préservé : dans la table générale
    Column("AR_REF",     String(50), primary_key=True, quote=True),
    # Préservé : dans la table générale
    Column("AR_DESIGN",  String(255), quote=True),
    # Modifié en String
    Column("AR_PRIXACH", String, quote=True),
    # Préservé : dans la table générale (clé pour la jointure)
    Column("FA_CODEFAMILLE", String(50), ForeignKey("FAMILLE.FA_CODEFAMILLE"), quote=True)
)

comptet_ventes = Table(
    "COMPTET", metadata_ventes,
    Column("CT_NUM",  String(50), primary_key=True, quote=True),
    Column("CT_INTITULE", String(255), quote=True),        
    Column("BT_NUM",  String, quote=True),
    Column("CA_NUM",  String, quote=True),
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
    Column("CT_DATEFERMEFIN", String, quote=True),
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

docligne_ventes = Table(
    "DOCLIGNE", metadata_ventes,
    Column("DL_NO", Integer, primary_key=True, quote=True),
    Column("AC_REFCLIENT", String(255), quote=True),   
    Column("AG_NO1", String, quote=True),
    Column("AG_NO2", String, quote=True),
    Column("AR_REF", String(50), ForeignKey("ARTICLES.AR_REF"), quote=True),
    Column("AR_REFCOMPOSE", String, quote=True),
    Column("CA_NUM", String, quote=True),
    Column("CO_NO", String, quote=True),
    Column("CT_NUM", String(50), ForeignKey("COMPTET.CT_NUM"), quote=True),
    Column("DE_NO", String, quote=True),
    Column("DLC", String, quote=True),
    Column("DLD", String, quote=True),
    Column("DL_CMUP", String, quote=True),
    Column("DL_DATEAVANCEMENT", String, quote=True),
    Column("DL_DATEBC", String, quote=True),
    Column("DL_DATEBL", TIMESTAMP, quote=True),        
    Column("DL_DATEPL", String, quote=True),
    Column("DL_DESIGN", String(255), quote=True),      
    Column("DL_ESCOMPTE", String, quote=True),
    Column("DL_FACTPOIDS", String, quote=True),
    Column("DL_FRAIS", String, quote=True),
    Column("DL_LIGNE", String, quote=True),
    Column("DL_MONTANTHT", Numeric(20, 6), quote=True),
    Column("DL_MONTANTTTC", String, quote=True),
    Column("DL_MVTSTOCK", String, quote=True),
    Column("DL_NOCOLIS", String, quote=True),
    Column("DL_NOLINK", String, quote=True),
    Column("DL_NONLIVRE", String, quote=True),
    Column("DL_NOREF", String, quote=True),
    Column("DL_PIECEBC", String(50), quote=True),      
    Column("DL_PIECEBL", String(50), quote=True),      
    Column("DL_PIECEPL", String, quote=True),
    Column("DL_POIDSBRUT", String, quote=True),
    Column("DL_POIDSNET", String, quote=True),
    Column("DL_PRIXRU", String, quote=True),
    Column("DL_PRIXUNITAIRE", Numeric(20, 6), quote=True),
    Column("DL_PUBC", String, quote=True),
    Column("DL_PUDEVISE", String, quote=True),
    Column("DL_PUTTC", String, quote=True),
    Column("DL_QTE", Numeric(20, 6), quote=True),      
    Column("DL_QTEBC", String, quote=True),
    Column("DL_QTEBL", String, quote=True),
    Column("DL_QTEPL", String, quote=True),
    Column("DL_QTERESSOURCE", String, quote=True),
    Column("DL_REMISE01REM_TYPE", String, quote=True),
    Column("DL_REMISE01REM_VALEUR", String, quote=True),
    Column("DL_REMISE02REM_TYPE", String, quote=True),
    Column("DL_REMISE02REM_VALEUR", String, quote=True),
    Column("DL_REMISE03REM_TYPE", String, quote=True),
    Column("DL_REMISE03REM_VALEUR", String, quote=True),
    Column("DL_TAXE1", String, quote=True),
    Column("DL_TAXE2", String, quote=True),
    Column("DL_TAXE3", String, quote=True),
    Column("DL_TNOMENCL", String, quote=True),
    Column("DL_TREMEXEP", String, quote=True),
    Column("DL_TREMPIED", String, quote=True),
    Column("DL_TTC", String, quote=True),
    Column("DL_TYPEPL", String, quote=True),
    Column("DL_TYPETAUX1", String, quote=True),
    Column("DL_TYPETAUX2", String, quote=True),
    Column("DL_TYPETAUX3", String, quote=True),
    Column("DL_TYPETAXE1", String, quote=True),
    Column("DL_TYPETAXE2", String, quote=True),
    Column("DL_TYPETAXE3", String, quote=True),
    Column("DL_VALORISE", String, quote=True),
    Column("DO_DATE", TIMESTAMP, quote=True),          
    Column("DO_DATELIVR", String, quote=True),
    Column("DO_DOMAINE", String, quote=True),
    Column("DO_PIECE", String, quote=True),
    Column("DO_REF", String, quote=True),
    Column("DO_TYPE", String, quote=True),
    Column("DT_NO", String, quote=True),
    Column("EU_ENUMERE", String, quote=True),
    Column("EU_QTE", String, quote=True),
    Column("FNT_MONTANTHT", String, quote=True),
    Column("FNT_MONTANTHTSIGNE", String, quote=True),
    Column("FNT_MONTANTTAXES", String, quote=True),
    Column("FNT_MONTANTTTC", String, quote=True),
    Column("FNT_MONTANTTTCSIGNE", String, quote=True),
    Column("FNT_PRIXUNET", String, quote=True),
    Column("FNT_PRIXUNETDEVISE", String, quote=True),
    Column("FNT_PRIXUNETTTC", String, quote=True),
    Column("FNT_QTESIGNE", String, quote=True),
    Column("FNT_REMISEGLOBALE", String, quote=True),
    Column("LS_COMPLEMENT", String, quote=True),
    Column("LS_FABRICATION", String, quote=True),
    Column("LS_NOSERIE", String, quote=True),
    Column("LS_PEREMPTION", String, quote=True),
    Column("N°_DE_LOT/_CURE_DATE", String, quote=True),
    Column("QTE_ACCESS", String, quote=True),
    Column("RP_CODE", String, quote=True)
)

# --------------------------------------------------------------------
# Création des tables Achats (réutilisation des définitions modifiées)
# --------------------------------------------------------------------
famille_achats = famille_ventes.to_metadata(metadata_achats)
articles_achats = articles_ventes.to_metadata(metadata_achats)
comptet_achats = comptet_ventes.to_metadata(metadata_achats)

fournisseur_achats = Table(
    "ARTFOURNISS", metadata_achats,
    Column("AF_REFFOURNISS", String(50), primary_key=True, quote=True),
    Column("AF_CODEBARRE", String, quote=True),
    Column("AF_COLISAGE", String, quote=True),
    Column("AF_CONVDIV", String, quote=True),
    Column("AF_CONVERSION", String, quote=True),
    Column("AF_DATEAPPLICATION", String, quote=True),
    Column("AF_DELAIAPPRO", String, quote=True),
    Column("AF_DEVISE", String, quote=True),
    Column("AF_GARANTIE", String, quote=True),
    Column("AF_PRINCIPAL", String, quote=True),
    Column("AF_PRIXACH", String, quote=True),
    Column("AF_PRIXACHNOUV", String, quote=True),
    Column("AF_PRIXDEV", String, quote=True),
    Column("AF_PRIXDEVNOUV", String, quote=True),
    Column("AF_QTEMINI", String, quote=True),
    Column("AF_QTEMONT", String, quote=True),
    Column("AF_REMISE", String, quote=True),
    Column("AF_REMISENOUV", String, quote=True),
    Column("AF_TYPEREM", String, quote=True),
    Column("AF_UNITE", String, quote=True),
    Column("AR_REF", String(50), ForeignKey("ARTICLES.AR_REF"), quote=True),
    Column("CT_NUM", String, quote=True),
    Column("EG_CHAMP", String, quote=True),
    Column("INT_CHAMP", String, quote=True),
    Column("INT_DEVISE", String, quote=True),
    Column("INT_UNITE", String, quote=True), 
    Column("CBCREATEUR", String, quote=True),
    Column("CBMODIFICATION", String, quote=True),
    Column("CBREPLICATION", String, quote=True)
)

# Créer la table DOCLIGNE pour les achats
# Les colonnes déjà modifiées de docligne_ventes sont copiées
colonnes_docligne_achats = [c.copy() for c in docligne_ventes.columns]
# Et la colonne AF_REFFOURNISS, qui doit être préservée, est ajoutée
colonnes_docligne_achats.append(
    Column("AF_REFFOURNISS", String(50), ForeignKey("ARTFOURNISS.AF_REFFOURNISS"), quote=True)
)
docligne_achats = Table("DOCLIGNE", metadata_achats, *colonnes_docligne_achats)