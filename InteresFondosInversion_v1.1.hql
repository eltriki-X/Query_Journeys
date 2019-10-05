

--El disparador son todos los clientes que han navegado por web, app o web de fondos en la parte de fondos de inversion
--Vamos a excluir todos aquellos que hayan navegado en la parte de empresas, o en mis productos(esto indica que no tienen interes en fondos, si no en su fondo)
WITH CODIGOS TITILARES AS (
        SELECT
		DISTINCT COPRBS
		FROM STANDARD.RELTIT
		WHERE SNAPSHOT = 'LATEST'),
	CONTRATACIONES_FONDOS AS (
		SELECT B.COPEMK
		FROM 
		(select COPSER, IDCOEC, IDPRIG, XCOEMP, COPEMK
		FROM 
		STANDARD.IGV0_FOND_INV_D
		WHERE SNAPSHOT = 'LATEST'
		AND COSIIG = 10000 AND FEIGFI IS NULL AND FFIGCO IS NULL  AND XCOEMP = 0) AS FONDOS
		LEFT JOIN
		(SELECT *
		FROM
		STANDARD.IGV0_CONTRATO_P A
		WHERE SNAPSHOT = 'LATEST' 
		AND XCOEMP = 0 AND FFIGCO IS NULL AND COSIIG = 10000 
		AND regexp_replace(A.COPRBS, '^0+','') IN (SELECT * FROM CODIGOS_TITULARES)) B
		ON B.COPSER = FONDOS.COPSER AND B.IDPRIG = FONDOS.IDPRIG AND B.XCOEMP = FONDOS.XCOEMP),
	EXPEDITION_DATE_MODELOS AS(
        SELECT 'TB1_MODELOPROPENMIN' AS C1, MAX(EXTRACTIONDATE) AS C2 FROM STANDARD.MODELOPROPENMIN WHERE SNAPSHOT = 'LATEST')		
SELECT 
A.ID_CLIENTE,
Case when TITULAR_FONDO IS NULL THEN 0 ELSE 1 END AS C0,--IND_TENENCIA_FI
CASE WHEN PROPENSION_SAS = 1 OR PROPENSION_BD = 1 THEN 1 ELSE 0 END AS C1,
A.CO_CANAL AS C2,
A.TL_SECCION1_PAGENAME AS C3 ,
A.TL_SECCION2_PAGENAME AS C4 ,
A.TL_SECCION3_PAGENAME AS C5,
A.TL_PROCESO_POST AS C6
FROM
(select 
ID_CLIENTE,
CO_CANAL,
TL_FAMILIA,
TL_SECCION0_PAGENAME,
TL_SECCION1_PAGENAME,
TL_SECCION2_PAGENAME,
TL_SECCION3_PAGENAME,
TL_SECCION4_PAGENAME,
TL_PROCESO_POST,
ROW_NUMBER() OVER(PARTITION BY ID_CLIENTE ORDER BY TL_PROCESO_POST <> 'contratacion' and TL_PROCESO_POST = '' ,TL_PROCESO_POST ASC,tl_seccion3_pagename = '', tl_seccion3_pagename desc)RN 
FROM 
derivatives_generic.bgtmd_analiticaweb_d
WHERE 
DT = '201909'--MAX DT
and FE_ADOBE_EXTRACTION_DIA = '20190924'--max FE_ADOBE_EXTRACTION_DIA 
AND TL_FAMILIA = 'Fondos de Inversion' AND TL_SECCION1_PAGENAME <> 'empresas' and TL_SECCION2_PAGENAME <> 'mis productos'
AND TL_PROCESO_POST NOT IN ('aportacion nacional fondo', 'movilizacion externa fondo', 'reembolso nacional fondo','traspaso fondo')
) A
where rn = 1
-- Nos quedamos solo con los clientes fisicos y activos
INNER JOIN 
(SELECT COPEMK
FROM STANDARD.MKTBBLON 
WHERE SNAPSHOT='LATEST' AND COTPMK = 1 and COH004 = 62120
) AS TABLON
ON ID_CLIENTE = TABLON.COPEMK
--Cruzamos con contrataciones fondos para saber si este cliente es titular de un fondo de inversion
LEFT JOIN
(SELECT COPEMK AS TITULAR_FONDO
FROM CONTRATACIONES_FONDOS)
ON CONTRATACIONES_FONDO.COPEMK = A.ID_CLIENTE
--Hay que cruzar con las tablas de propensiones para generar el indicador de cliente propenso a contratar fondo de inversion
LEFT JOIN
(SELECT IDPERS, 
1 AS PROPENSION_SAS
FROM (SELECT IDPERS, nupropen, idmode FROM standard.crpropen AS CRP
WHERE  CRP.IDMODE in ('FIAPOR1','TNFIASE01','TNFIMRF01','TRENTFI01','XSFIGAR01','XSFIRF01') AND CRP.extractiondate  in ( SELECT max(C2) FROM EXPEDITION_DATE_MODELOS )  ) AS crpropension,
(select idmode,
iduso,
nupropenmin
from 
standard.modelopropenmin AS MOP
WHERE MOP.idmode in ('FIAPOR1','TNFIASE01','TNFIMRF01','TRENTFI01','XSFIGAR01','XSFIRF01') AND MOP.iduso='MEDICION' AND MOP.extractiondate in ( SELECT max(C2) FROM EXPEDITION_DATE_MODELOS )  ) AS propension  
WHERE 
 crpropension.nupropen>= propension.nupropenmin AND crpropension.idmode =propension.idmode
GROUP BY IDPERS) AS F
ON A.ID_CLIENTE = F.IDPERS
LEFT JOIN
--Comprobar que no sale mal porque he metido mas de un modelo he metido el = en el where para que no se lie
(SELECT CO_CLIENTE,
1 AS PROPENSION_BD
FROM ( select CO_CLIENTE,probabilidad,MODELID  from derivatives_models_in.bgmd_in_matriz_propensiones AS  MP
where MP.modelid IN ('MFOGENU1','MFOTRAU1','MFOTRAX1') AND MP.sessionid in ( SELECT max(C2) FROM EXPEDITION_DATE_MODELOS) ) AS MATRIZ_PROP,
(select nupropenmin, IDMODE
FROM standard.modelopropenmin AS MP2
WHERE MP2.iduso='MEDICION' AND MP2.IDMODE in('MFOGENU1','MFOTRAU1','MFOTRAX1') AND MP2.extractiondate in ( SELECT max(C2) FROM EXPEDITION_DATE_MODELOS ) ) AS MODELO_PROPENSION  
WHERE  MATRIZ_PROP.probabilidad>= MODELO_PROPENSION.nupropenmin AND MATRIZ_PROP.MODELID = MODELO_PROPENSION.idmode
GROUP BY CO_CLIENTE ) AS G 
ON A.IN_CLIENTE = G.CO_CLIENTE
;

