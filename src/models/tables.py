# -- coding: utf-8 --

from sqlalchemy import (
    MetaData, Table, Column, Numeric,
    Integer, ForeignKey, TIMESTAMP
)
from sqlalchemy.types import TypeDecorator, VARCHAR, Text as SAText

# --------------------------------------------------------------------
# Type personnalisé pour choisir VARCHAR ou TEXT selon le dialecte
# Objectif : Utiliser TEXT pour les champs de longueur indéfinie sur MySQL
# et VARCHAR sur PostgreSQL pour une meilleure performance.
# --------------------------------------------------------------------
class VarCharOrText(TypeDecorator):
    """
    Type qui choisit dynamiquement entre VARCHAR et TEXT.
    - Pour MySQL:
        - Utilise TEXT si aucune longueur n'est spécifiée.
        - Utilise VARCHAR(longueur) si une longueur est spécifiée.
    - Pour les autres dialectes (ex: PostgreSQL):
        - Utilise VARCHAR (sans longueur) si aucune longueur n'est spécifiée.
        - Utilise VARCHAR(longueur) si une longueur est spécifiée.
    """
    impl = VARCHAR

    def __init__(self, length=None):
        super(VarCharOrText, self).__init__()
        self.length = length

    def load_dialect_impl(self, dialect):
        if dialect.name == 'mysql':
            if self.length is None:
                return dialect.type_descriptor(SAText())
            else:
                return dialect.type_descriptor(VARCHAR(self.length))
        else:
            return dialect.type_descriptor(VARCHAR(self.length))

# --------------------------------------------------------------------
# Déclaration des métadonnées SANS information de schéma
# --------------------------------------------------------------------
metadata_ventes = MetaData()
metadata_achats = MetaData()

# --------------------------------------------------------------------
# Création des tables Ventes
# Les types spécifiques (Numeric, Integer, TIMESTAMP) sont préservés
# pour les colonnes des "tables générales". Les autres utilisent VarCharOrText.
# --------------------------------------------------------------------

famille_ventes = Table(
    "FAMILLE", metadata_ventes,
    Column("FA_CODEFAMILLE", VarCharOrText(50), primary_key=True, quote=True), # Préservé
    Column("FA_CENTRAL",     VarCharOrText(50), quote=True),                  # Préservé
    Column("FA_INTITULE",    VarCharOrText(255), nullable=False, quote=True) # Préservé
)

article_ventes = Table(
    "ARTICLE", metadata_ventes,
    Column("AR_REF",     VarCharOrText(50), primary_key=True, quote=True), # Préservé
    Column("AR_DESIGN",  VarCharOrText(255), quote=True),                  # Préservé
    Column("AR_PRIXACH", VarCharOrText(), quote=True), # Modifié en texte
    Column("FA_CODEFAMILLE", VarCharOrText(50), ForeignKey("FAMILLE.FA_CODEFAMILLE"), quote=True) # Préservé (FK)
)

comptet_ventes = Table(
    "COMPTET", metadata_ventes,
    Column("CT_NUM",  VarCharOrText(50), primary_key=True, quote=True), # Préservé
    Column("CT_INTITULE", VarCharOrText(255), quote=True),             # Préservé
    Column("BT_NUM", VarCharOrText(), quote=True),
    Column("CA_NUM", VarCharOrText(), quote=True),
    Column("CA_NUMIFRS", VarCharOrText(), quote=True),
    Column("CBCREATEUR", VarCharOrText(), quote=True),
    Column("CBMODIFICATION", VarCharOrText(), quote=True),
    Column("CBREPLICATION", VarCharOrText(), quote=True),
    Column("CG_NUMPRINC", VarCharOrText(), quote=True),
    Column("CODE_HYPERIX_CHEZ_LE_CLIENT", VarCharOrText(), quote=True),
    Column("CO_NO", VarCharOrText(), quote=True),
    Column("CT_ADRESSE", VarCharOrText(), quote=True),
    Column("CT_APE", VarCharOrText(), quote=True),
    Column("CT_ASSURANCE", VarCharOrText(), quote=True),
    Column("CT_BLFACT", VarCharOrText(), quote=True),
    Column("CT_CLASSEMENT", VarCharOrText(), quote=True),
    Column("CT_CODEPOSTAL", VarCharOrText(), quote=True),
    Column("CT_CODEREGION", VarCharOrText(), quote=True),
    Column("CT_COFACE", VarCharOrText(), quote=True),
    Column("CT_COMMENTAIRE", VarCharOrText(), quote=True),
    Column("CT_COMPLEMENT", VarCharOrText(), quote=True),
    Column("CT_CONTACT", VarCharOrText(), quote=True),
    Column("CT_CONTROLENC", VarCharOrText(), quote=True),
    Column("CT_DATECREATE", VarCharOrText(), quote=True),
    Column("CT_DATEFERMEDEBUT", VarCharOrText(), quote=True),
    Column("CT_DATEFERMEFIN", VarCharOrText(), quote=True),
    Column("CT_EDI1", VarCharOrText(), quote=True),
    Column("CT_EDI2", VarCharOrText(), quote=True),
    Column("CT_EDI3", VarCharOrText(), quote=True),
    Column("CT_EMAIL", VarCharOrText(), quote=True),
    Column("CT_ENCOURS", VarCharOrText(), quote=True),
    Column("CT_FACTURE", VarCharOrText(), quote=True),
    Column("CT_FACTUREELEC", VarCharOrText(), quote=True),
    Column("CT_IDENTIFIANT", VarCharOrText(), quote=True),
    Column("CT_LANGUE", VarCharOrText(), quote=True),
    Column("CT_LETTRAGE", VarCharOrText(), quote=True),
    Column("CT_LIVRPARTIELLE", VarCharOrText(), quote=True),
    Column("CT_NOTPENAL", VarCharOrText(), quote=True),
    Column("CT_NOTRAPPEL", VarCharOrText(), quote=True),
    Column("CT_NUMCENTRALE", VarCharOrText(), quote=True),
    Column("CT_NUMPAYEUR", VarCharOrText(), quote=True),
    Column("CT_PAYS", VarCharOrText(), quote=True),
    Column("CT_PRIORITELIVR", VarCharOrText(), quote=True),
    Column("CT_QUALITE", VarCharOrText(), quote=True),
    Column("CT_RACCOURCI", VarCharOrText(), quote=True),
    Column("CT_REPRESENTINT", VarCharOrText(), quote=True),
    Column("CT_REPRESENTNIF", VarCharOrText(), quote=True),
    Column("CT_SAUT", VarCharOrText(), quote=True),
    Column("CT_SIRET", VarCharOrText(), quote=True),
    Column("CT_SITE", VarCharOrText(), quote=True),
    Column("CT_SOMMEIL", VarCharOrText(), quote=True),
    Column("CT_STATISTIQUE01", VarCharOrText(), quote=True),
    Column("CT_STATISTIQUE02", VarCharOrText(), quote=True),
    Column("CT_STATISTIQUE03", VarCharOrText(), quote=True),
    Column("CT_STATISTIQUE04", VarCharOrText(), quote=True),
    Column("CT_STATISTIQUE05", VarCharOrText(), quote=True),
    Column("CT_STATISTIQUE06", VarCharOrText(), quote=True),
    Column("CT_STATISTIQUE07", VarCharOrText(), quote=True),
    Column("CT_STATISTIQUE08", VarCharOrText(), quote=True),
    Column("CT_STATISTIQUE09", VarCharOrText(), quote=True),
    Column("CT_STATISTIQUE10", VarCharOrText(), quote=True),
    Column("CT_SURVEILLANCE", VarCharOrText(), quote=True),
    Column("CT_SVCA", VarCharOrText(), quote=True),
    Column("CT_SVCOTATION", VarCharOrText(), quote=True),
    Column("CT_SVDATEBILAN", VarCharOrText(), quote=True),
    Column("CT_SVDATECREATE", VarCharOrText(), quote=True),
    Column("CT_SVDATEINCID", VarCharOrText(), quote=True),
    Column("CT_SVDATEMAJ", VarCharOrText(), quote=True),
    Column("CT_SVEFFECTIF", VarCharOrText(), quote=True),
    Column("CT_SVFORMEJURI", VarCharOrText(), quote=True),
    Column("CT_SVINCIDENT", VarCharOrText(), quote=True),
    Column("CT_SVNBMOISBILAN", VarCharOrText(), quote=True),
    Column("CT_SVOBJETMAJ", VarCharOrText(), quote=True),
    Column("CT_SVPRIVIL", VarCharOrText(), quote=True),
    Column("CT_SVREGUL", VarCharOrText(), quote=True),
    Column("CT_SVRESULTAT", VarCharOrText(), quote=True),
    Column("CT_TAUX01", VarCharOrText(), quote=True),
    Column("CT_TAUX02", VarCharOrText(), quote=True),
    Column("CT_TAUX03", VarCharOrText(), quote=True),
    Column("CT_TAUX04", VarCharOrText(), quote=True),
    Column("CT_TELECOPIE", VarCharOrText(), quote=True),
    Column("CT_TELEPHONE", VarCharOrText(), quote=True),
    Column("CT_TYPE", VarCharOrText(), quote=True),
    Column("CT_TYPENIF", VarCharOrText(), quote=True),
    Column("CT_VALIDECH", VarCharOrText(), quote=True),
    Column("CT_VILLE", VarCharOrText(), quote=True),
    Column("DE_NO", VarCharOrText(), quote=True),
    Column("EB_NO", VarCharOrText(), quote=True),
    Column("INT_ANALYTIQUE", VarCharOrText(), quote=True),
    Column("INT_CATCOMPTA", VarCharOrText(), quote=True),
    Column("INT_CATTARIF", VarCharOrText(), quote=True),
    Column("INT_CONDITION", VarCharOrText(), quote=True),
    Column("INT_DEVISE", VarCharOrText(), quote=True),
    Column("INT_EXPEDITION", VarCharOrText(), quote=True),
    Column("INT_PERIOD", VarCharOrText(), quote=True),
    Column("INT_RISQUE", VarCharOrText(), quote=True),
    Column("MR_NO", VarCharOrText(), quote=True),
    Column("N_ANALYTIQUE", VarCharOrText(), quote=True),
    Column("N_ANALYTIQUEIFRS", VarCharOrText(), quote=True),
    Column("N_CATCOMPTA", VarCharOrText(), quote=True),
    Column("N_CATTARIF", VarCharOrText(), quote=True),
    Column("N_CONDITION", VarCharOrText(), quote=True),
    Column("N_DEVISE", VarCharOrText(), quote=True),
    Column("N_EXPEDITION", VarCharOrText(), quote=True),
    Column("N_PERIOD", VarCharOrText(), quote=True),
    Column("N_RISQUE", VarCharOrText(), quote=True)
)

docligne_ventes = Table(
    "DOCLIGNE", metadata_ventes,
    Column("DL_NO", Integer, primary_key=True, quote=True),           # Préservé
    Column("AC_REFCLIENT", VarCharOrText(255), quote=True),           # Préservé
    Column("AG_NO1", VarCharOrText(), quote=True),
    Column("AG_NO2", VarCharOrText(), quote=True),
    Column("AR_REF", VarCharOrText(50), ForeignKey("ARTICLE.AR_REF"), quote=True), # Préservé
    Column("AR_REFCOMPOSE", VarCharOrText(), quote=True),
    Column("CA_NUM", VarCharOrText(), quote=True),
    Column("CO_NO", VarCharOrText(), quote=True),
    Column("CT_NUM", VarCharOrText(50), ForeignKey("COMPTET.CT_NUM"), quote=True), # Préservé
    Column("DE_NO", VarCharOrText(), quote=True),
    Column("DLC", VarCharOrText(), quote=True),
    Column("DLD", VarCharOrText(), quote=True),
    Column("DL_CMUP", VarCharOrText(), quote=True),
    Column("DL_DATEAVANCEMENT", VarCharOrText(), quote=True),
    Column("DL_DATEBC", VarCharOrText(), quote=True),
    Column("DL_DATEBL", TIMESTAMP, quote=True),                        # Préservé
    Column("DL_DATEPL", VarCharOrText(), quote=True),
    Column("DL_DESIGN", VarCharOrText(255), quote=True),              # Préservé
    Column("DL_ESCOMPTE", VarCharOrText(), quote=True),
    Column("DL_FACTPOIDS", VarCharOrText(), quote=True),
    Column("DL_FRAIS", VarCharOrText(), quote=True),
    Column("DL_LIGNE", VarCharOrText(), quote=True),
    Column("DL_MONTANTHT", Numeric(20, 6), quote=True),               # Préservé
    Column("DL_MONTANTTTC", VarCharOrText(), quote=True),
    Column("DL_MVTSTOCK", VarCharOrText(), quote=True),
    Column("DL_NOCOLIS", VarCharOrText(), quote=True),
    Column("DL_NOLINK", VarCharOrText(), quote=True),
    Column("DL_NONLIVRE", VarCharOrText(), quote=True),
    Column("DL_NOREF", VarCharOrText(), quote=True),
    Column("DL_PIECEBC", VarCharOrText(50), quote=True),              # Préservé
    Column("DL_PIECEBL", VarCharOrText(50), quote=True),              # Préservé
    Column("DL_PIECEPL", VarCharOrText(), quote=True),
    Column("DL_POIDSBRUT", VarCharOrText(), quote=True),
    Column("DL_POIDSNET", VarCharOrText(), quote=True),
    Column("DL_PRIXRU", VarCharOrText(), quote=True),
    Column("DL_PRIXUNITAIRE", Numeric(20, 6), quote=True),            # Préservé
    Column("DL_PUBC", VarCharOrText(), quote=True),
    Column("DL_PUDEVISE", VarCharOrText(), quote=True),
    Column("DL_PUTTC", VarCharOrText(), quote=True),
    Column("DL_QTE", Numeric(20, 6), quote=True),                      # Préservé
    Column("DL_QTEBC", VarCharOrText(), quote=True),
    Column("DL_QTEBL", VarCharOrText(), quote=True),
    Column("DL_QTEPL", VarCharOrText(), quote=True),
    Column("DL_QTERESSOURCE", VarCharOrText(), quote=True),
    Column("DL_REMISE01REM_TYPE", VarCharOrText(), quote=True),
    Column("DL_REMISE01REM_VALEUR", VarCharOrText(), quote=True),
    Column("DL_REMISE02REM_TYPE", VarCharOrText(), quote=True),
    Column("DL_REMISE02REM_VALEUR", VarCharOrText(), quote=True),
    Column("DL_REMISE03REM_TYPE", VarCharOrText(), quote=True),
    Column("DL_REMISE03REM_VALEUR", VarCharOrText(), quote=True),
    Column("DL_TAXE1", VarCharOrText(), quote=True),
    Column("DL_TAXE2", VarCharOrText(), quote=True),
    Column("DL_TAXE3", VarCharOrText(), quote=True),
    Column("DL_TNOMENCL", VarCharOrText(), quote=True),
    Column("DL_TREMEXEP", VarCharOrText(), quote=True),
    Column("DL_TREMPIED", VarCharOrText(), quote=True),
    Column("DL_TTC", VarCharOrText(), quote=True),
    Column("DL_TYPEPL", VarCharOrText(), quote=True),
    Column("DL_TYPETAUX1", VarCharOrText(), quote=True),
    Column("DL_TYPETAUX2", VarCharOrText(), quote=True),
    Column("DL_TYPETAUX3", VarCharOrText(), quote=True),
    Column("DL_TYPETAXE1", VarCharOrText(), quote=True),
    Column("DL_TYPETAXE2", VarCharOrText(), quote=True),
    Column("DL_TYPETAXE3", VarCharOrText(), quote=True),
    Column("DL_VALORISE", VarCharOrText(), quote=True),
    Column("DO_DATE", TIMESTAMP, quote=True),                          # Préservé
    Column("DO_DATELIVR", VarCharOrText(), quote=True),
    Column("DO_DOMAINE", VarCharOrText(), quote=True),
    Column("DO_PIECE", VarCharOrText(), quote=True),
    Column("DO_REF", VarCharOrText(), quote=True),
    Column("DO_TYPE", VarCharOrText(), quote=True),
    Column("DT_NO", VarCharOrText(), quote=True),
    Column("EU_ENUMERE", VarCharOrText(), quote=True),
    Column("EU_QTE", VarCharOrText(), quote=True),
    Column("FNT_MONTANTHT", VarCharOrText(), quote=True),
    Column("FNT_MONTANTHTSIGNE", VarCharOrText(), quote=True),
    Column("FNT_MONTANTTAXES", VarCharOrText(), quote=True),
    Column("FNT_MONTANTTTC", VarCharOrText(), quote=True),
    Column("FNT_MONTANTTTCSIGNE", VarCharOrText(), quote=True),
    Column("FNT_PRIXUNET", VarCharOrText(), quote=True),
    Column("FNT_PRIXUNETDEVISE", VarCharOrText(), quote=True),
    Column("FNT_PRIXUNETTTC", VarCharOrText(), quote=True),
    Column("FNT_QTESIGNE", VarCharOrText(), quote=True),
    Column("FNT_REMISEGLOBALE", VarCharOrText(), quote=True),
    Column("LS_COMPLEMENT", VarCharOrText(), quote=True),
    Column("LS_FABRICATION", VarCharOrText(), quote=True),
    Column("LS_NOSERIE", VarCharOrText(), quote=True),
    Column("LS_PEREMPTION", VarCharOrText(), quote=True),
    Column("N°_DE_LOT/_CURE_DATE", VarCharOrText(), quote=True),
    Column("QTE_ACCESS", VarCharOrText(), quote=True),
    Column("RP_CODE", VarCharOrText(), quote=True)
)

# --------------------------------------------------------------------
# Création des tables Achats (réutilisation des définitions modifiées)
# --------------------------------------------------------------------
famille_achats = famille_ventes.to_metadata(metadata_achats)
article_achats = article_ventes.to_metadata(metadata_achats)
comptet_achats = comptet_ventes.to_metadata(metadata_achats)

fournisseur_achats = Table(
    "ARTFOURNISS", metadata_achats,
    Column("AF_REFFOURNISS", VarCharOrText(50), primary_key=True, quote=True), # Préservé
    Column("AF_CODEBARRE", VarCharOrText(), quote=True),
    Column("AF_COLISAGE", VarCharOrText(), quote=True),
    Column("AF_CONVDIV", VarCharOrText(), quote=True),
    Column("AF_CONVERSION", VarCharOrText(), quote=True),
    Column("AF_DATEAPPLICATION", VarCharOrText(), quote=True),
    Column("AF_DELAIAPPRO", VarCharOrText(), quote=True),
    Column("AF_DEVISE", VarCharOrText(), quote=True),
    Column("AF_GARANTIE", VarCharOrText(), quote=True),
    Column("AF_PRINCIPAL", VarCharOrText(), quote=True),
    Column("AF_PRIXACH", VarCharOrText(), quote=True),
    Column("AF_PRIXACHNOUV", VarCharOrText(), quote=True),
    Column("AF_PRIXDEV", VarCharOrText(), quote=True),
    Column("AF_PRIXDEVNOUV", VarCharOrText(), quote=True),
    Column("AF_QTEMINI", VarCharOrText(), quote=True),
    Column("AF_QTEMONT", VarCharOrText(), quote=True),
    Column("AF_REMISE", VarCharOrText(), quote=True),
    Column("AF_REMISENOUV", VarCharOrText(), quote=True),
    Column("AF_TYPEREM", VarCharOrText(), quote=True),
    Column("AF_UNITE", VarCharOrText(), quote=True),
    Column("AR_REF", VarCharOrText(50), ForeignKey("ARTICLE.AR_REF"), quote=True), # Préservé
    Column("CT_NUM", VarCharOrText(), quote=True),
    Column("EG_CHAMP", VarCharOrText(), quote=True),
    Column("INT_CHAMP", VarCharOrText(), quote=True),
    Column("INT_DEVISE", VarCharOrText(), quote=True),
    Column("INT_UNITE", VarCharOrText(), quote=True),
    Column("CBCREATEUR", VarCharOrText(), quote=True),
    Column("CBMODIFICATION", VarCharOrText(), quote=True),
    Column("CBREPLICATION", VarCharOrText(), quote=True)
)

# Créer la table DOCLIGNE pour les achats en ajoutant la clé étrangère manquante
colonnes_docligne_achats = [c.copy() for c in docligne_ventes.columns]
colonnes_docligne_achats.append(
    Column("AF_REFFOURNISS", VarCharOrText(50), ForeignKey("ARTFOURNISS.AF_REFFOURNISS"), quote=True)
)
docligne_achats = Table("DOCLIGNE", metadata_achats, *colonnes_docligne_achats)