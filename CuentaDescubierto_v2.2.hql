WITH 
    EXPEDITION_DISPARADOR AS(
        SELECT
                               ID_URSUS,
							   CONCAT(A.COPSER,A.XCOEMP,A.IDPRIG) AS CODIGO_CUENTA,
							   substring(A.IDPRIG,-4)  AS NUMERO_CUENTA,
							   IMPORTE_DESCUBIERTO,
                               CASE WHEN IMPORTE_DESCUBIERTO < 0 THEN 1 ELSE 0 END AS IND_DESCUBIERTO,
                               CASE WHEN SALDO_AYER >= 0 THEN 1 ELSE 0 END AS IND_AYER_CUBIERTO,
                               FECHA_DESCUBIERTO,
                               IND_RECEPTORA_NOMINA,
                               IND_RECEPTORA_PENSION,
                               IND_RECEPTORA_DESEMPLEO,
                               CASE WHEN IMPORTE_LIMITE_DESCUBIERTO >= IMPORTE_DESCUBIERTO THEN 1 ELSE 0 END AS IND_SERV_DESCUBIERTO_CUBRE,
							   A.XCOEMP, 
							   A.COPSER,
							   A.IDCOEC,
							   A.IDPRIG
                               FROM 
                               (SELECT 
							   ID_URSUS,
							   AA.IMPORTE_DESCUBIERTO,
							   AA.FECHA_DESCUBIERTO,
							   AA.IND_RECEPTORA_NOMINA,
							   AA.IND_RECEPTORA_PENSION,
							   AA.IND_RECEPTORA_DESEMPLEO,
							   AA.IMPORTE_LIMITE_DESCUBIERTO,
							   AA.XCOEMP, 
							   AA.COPSER,
							   AA.IDCOEC,
							   AA.IDPRIG
							   FROM
							   (select 
							   COPEMK AS CODIGO_TITULAR_PRINCIPAL,
							   CAVT20 AS IMPORTE_DESCUBIERTO,
							   FEVAIG AS FECHA_DESCUBIERTO,
							   INZ120 AS IND_RECEPTORA_NOMINA,
							   INZ121 AS IND_RECEPTORA_PENSION,
							   INZ122 AS IND_RECEPTORA_DESEMPLEO,
							   IMLIMK AS IMPORTE_LIMITE_DESCUBIERTO,
							   XCOEMP, 
							   COPSER,
							   IDCOEC,
							   IDPRIG
							   FROM  STANDARD.IGV0_CP_AHCA_D
							   WHERE SNAPSHOT='LATEST'AND EXTRACTIONDATE = date_format(date_sub(current_date(),1), 'yyyyMMdd')  AND CAVT20 < 0 AND COSIIG = 10000 AND FEIGFI IS NULL AND FFIGCO IS NULL  AND XCOEMP = 0) AS AA
							   LEFT JOIN
							   (SELECT COPEMK AS ID_URSUS,
							   COPSER,
							   IDPRIG,
							   XCOEMP
							   FROM STANDARD.IGV0_CONTRATO_P
							   WHERE SNAPSHOT = 'LATEST' AND COPRBS = 20 AND XCOEMP = 0  AND FFIGCO IS NULL AND COSIIG = 10000 ) AS CONTRATOS
							   ON AA.COPSER =CONTRATOS.COPSER AND  AA.IDPRIG = CONTRATOS.IDPRIG AND AA.XCOEMP = CONTRATOS.XCOEMP) AS A 
                               LEFT JOIN
                               (SELECT CAVT20 as SALDO_AYER, XCOEMP, COPSER, IDCOEC,IDPRIG 
                               FROM STANDARD.IGV0_CP_AHCA_D
                               WHERE SNAPSHOT = 'HISTORIC'  AND EXTRACTIONDATE = date_format(date_sub(current_date(),2), 'yyyyMMdd') AND COSIIG = 10000 AND FEIGFI IS NULL AND FFIGCO IS NULL  AND XCOEMP = 0 AND CAVT20 > 0 ) AS B
                               ON A.XCOEMP = B.XCOEMP AND A.COPSER = B.COPSER AND A.IDCOEC = B.IDCOEC AND A.IDPRIG = B.IDPRIG
                               INNER JOIN 
                               (SELECT COPEMK
                               FROM STANDARD.MKTBBLON 
                               WHERE SNAPSHOT='LATEST' AND COTPMK = 1) AS TABLON
                               ON A.ID_URSUS = TABLON.COPEMK
                               WHERE SALDO_AYER > 0
                               ),
                                  EXPEDITION_DATE_MODELOS AS(
        SELECT 'TB1_MODELOPROPENMIN' AS C1, MAX(EXTRACTIONDATE) AS C2 FROM STANDARD.MODELOPROPENMIN WHERE SNAPSHOT = 'LATEST')
SELECT
A.ID_URSUS,
A.NUMERO_CUENTA AS C0,
IND_SALDO_TOTAL_CUENTAS_CUBRE_DESCUBIERTO AS C1,
A.IND_SERV_DESCUBIERTO_CUBRE AS C2,
CASE WHEN MAX_LIMITE_DISPONIBLE_TARJ_CRED > A.IMPORTE_DESCUBIERTO THEN 1 ELSE 0 END AS IND_LIMITE_TARJ_CUBRE_DESCUBIERTO AS C3,
CASE WHEN (IMPORTE_LPP >= A.IMPORTE_DESCUBIERTO  AND VIGENCIA_LPP > FECHA_DESCUBIERTO) then 1 else 0 END AS IND_LPP_CUBRE_DESCUBIERTO AS C4,
CASE WHEN PROPENSION_LPP_SAS = 1 OR PROPENSION_LPP_BD = 1 THEN 1 ELSE 0 END AS C5,
COALESCE(IND_ANTICIPO_NOMINA,0) AS C6
md5(A.CODIGO_CUENTA) AS C7
FROM 
(SELECT
ID_URSUS,
CODIGO_CUENTA,
NUMERO_CUENTA,
IMPORTE_DESCUBIERTO,
IND_DESCUBIERTO,
IND_AYER_CUBIERTO,
FECHA_DESCUBIERTO,
IND_RECEPTORA_NOMINA,
IND_RECEPTORA_PENSION,
IND_RECEPTORA_DESEMPLEO,
IND_SERV_DESCUBIERTO_CUBRE,
XCOEMP, 
COPSER,
IDCOEC,
IDPRIG
FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY ID_URSUS ORDER BY IMPORTE_DESCUBIERTO ASC ) AS RN
FROM
EXPEDITION_DISPARADOR ) AS A_RN
WHERE RN = 1 ) as A
LEFT JOIN
(
SELECT 
CONTRATOS.COPEMK,
-- Hacemos el sumatorio de todas las cuentas, ya que nos interesa conocer su saldo final, si resulta ser positivo es que puede cubrir el descubierto con el saldo que tenga en otras cuentas
CASE WHEN SUM(CUENTAS.CAVT20)> 0 THEN 1 ELSE 0  END AS IND_SALDO_TOTAL_CUENTAS_CUBRE_DESCUBIERTO
FROM 
 EXPEDITION_DISPARADOR 
 LEFT JOIN 
 (SELECT 
 COPEMK,
COPSER,
IDPRIG,
XCOEMP,
COSIIG
FROM 
 STANDARD.IGV0_CONTRATO_P
WHERE SNAPSHOT = 'LATEST' AND COPRBS = 20 AND XCOEMP = 0  AND FFIGCO IS NULL AND COSIIG = 10000
) AS CONTRATOS
ON EXPEDITION_DISPARADOR.ID_URSUS = CONTRATOS.COPEMK
LEFT JOIN
(SELECT
XCOEMP, 
 COPSER,
IDCOEC,
IDPRIG,
CAVT20
FROM
STANDARD.IGV0_CP_AHCA_D
WHERE SNAPSHOT='LATEST' AND COSIIG = 10000 AND FEIGFI IS NULL AND FFIGCO IS NULL  AND XCOEMP = 0) AS CUENTAS 
 ON  CONTRATOS.COPSER = CUENTAS.COPSER AND CONTRATOS.IDPRIG = CUENTAS.IDPRIG AND CONTRATOS.XCOEMP = CUENTAS.XCOEMP
GROUP BY CONTRATOS.COPEMK
) AS C
ON A.ID_URSUS = C.COPEMK
LEFT JOIN
(SELECT COPEMK,
MAX(CACV0D) AS MAX_LIMITE_DISPONIBLE_TARJ_CRED
FROM STANDARD.IGV0_TARJETA_D
WHERE 
SNAPSHOT='LATEST' AND CACV0D > 0 AND COSIIG = 10000 AND FEIGFI IS NULL AND FFIGCO IS NULL  AND XCOEMP = 0 AND TRIM(TITAR3) = 'C' AND PORCPA= 100
GROUP BY 
COPEMK
) AS D
ON A.ID_URSUS = D.COPEMK
LEFT JOIN 
(select copemk, 
IMLPPM AS IMPORTE_LPP,
MAX(FECLPP) AS VIGENCIA_LPP,
IF(INANOM IN (1,2), 1, 0) AS IND_ANTICIPO_NOMINA
from STANDARD.IGV0_LPRESPRE_D 
where SNAPSHOT='LATEST' and XCOEMP = 0 AND FEIGFI IS NULL AND FFIGCO IS NULL AND COSIIG = 10000 AND TRIM(COELPP) = 'AC' AND TRIM(COESPQ)='VI' AND TRIM( CDTCLS) <>'99' and copser=93010  
GROUP BY copemk,IMLPPM,INANOM ) AS E 
ON A.ID_URSUS = E.COPEMK
LEFT JOIN
(SELECT IDPERS, 
1 AS PROPENSION_LPP_SAS
FROM (SELECT IDPERS, nupropen FROM standard.crpropen AS CRP
WHERE  CRP.IDMODE='PCONSLPP02' AND CRP.extractiondate  in ( SELECT max(C2) FROM EXPEDITION_DATE_MODELOS )  ) AS crpropension,
(select idmode,
iduso,
nupropenmin
from 
standard.modelopropenmin AS MOP
WHERE MOP.idmode = 'PCONSLPP02' AND MOP.iduso='MEDICION' AND MOP.extractiondate in ( SELECT max(C2) FROM EXPEDITION_DATE_MODELOS )  ) AS propension  
WHERE 
 crpropension.nupropen>= propension.nupropenmin
GROUP BY IDPERS) AS F
ON A.ID_URSUS = F.IDPERS
LEFT JOIN
(SELECT CO_CLIENTE,
1 AS PROPENSION_LPP_BD
FROM ( select CO_CLIENTE,probabilidad  from derivatives_models_in.bgmd_in_matriz_propensiones AS  MP
where MP.modelid='MCOCLFX1' AND MP.sessionid in ( SELECT max(C2) FROM EXPEDITION_DATE_MODELOS) ) AS MATRIZ_PROP,
(select nupropenmin
FROM standard.modelopropenmin AS MP2
WHERE MP2.iduso='MEDICION' AND MP2.IDMODE ='MCOCLFX1' AND MP2.extractiondate in ( SELECT max(C2) FROM EXPEDITION_DATE_MODELOS ) ) AS MODELO_PROPENSION  
WHERE  MATRIZ_PROP.probabilidad>= MODELO_PROPENSION.nupropenmin
GROUP BY CO_CLIENTE ) AS G 
ON A.ID_URSUS = G.CO_CLIENTE
;
