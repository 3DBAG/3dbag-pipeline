from dataclasses import dataclass
from typing import Optional


@dataclass
class Attributes:
    b3_bag_bag_overlap: float
    b3_dak_type: str
    b3_h_dak_50p: float
    b3_h_dak_70p: float
    b3_h_dak_max: float
    b3_h_dak_min: float
    b3_h_maaiveld: float
    b3_kas_warenhuis: bool
    b3_mutatie_ahn3_ahn4: bool
    b3_nodata_fractie_ahn3: float
    b3_nodata_fractie_ahn4: float
    b3_nodata_radius_ahn3: float
    b3_nodata_radius_ahn4: float
    b3_puntdichtheid_ahn3: float
    b3_puntdichtheid_ahn4: float
    b3_pw_bron: str
    b3_pw_datum: int
    b3_pw_selectie_reden: str
    b3_reconstructie_onvolledig: bool
    b3_rmse_lod12: float
    b3_rmse_lod13: float
    b3_rmse_lod22: float
    b3_val3dity_lod12: str
    b3_val3dity_lod13: str
    b3_val3dity_lod22: str
    b3_volume_lod12: float
    b3_volume_lod13: float
    b3_volume_lod22: float
    begingeldigheid: str
    documentdatum: str
    documentnummer: str
    eindgeldigheid: Optional[str]
    eindregistratie: Optional[str]
    geconstateerd: bool
    identificatie: str
    oorspronkelijkbouwjaar: int
    status: str
    tijdstipeindregistratielv: str
    tijdstipinactief: str
    tijdstipinactieflv: str
    tijdstipnietbaglv: str
    tijdstipregistratie: str
    tijdstipregistratielv: str
    voorkomenidentificatie: str
    b3_opp_grond: float
    b3_opp_dak_plat: float
    b3_opp_dak_schuin: float
    b3_opp_scheidingsmuur: float
    b3_opp_buitenmuur: float
