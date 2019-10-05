WITH 
    CODIGOS_TITULARES AS(
        SELECT
		DISTINCT COPRBS
		FROM STANDARD.RELTIT
		WHERE SNAPSHOT = 'LATEST'
		)
SELECT
	B.COPSER AS URSUS_PRODUCTO,
	descrip_valor AS DESC_PRODUCTO,
	B.COPRBS AS TIPO_RELACION,
	B.COPEMK AS URSUS_CLIENTE,
	PRODUCTOS.CO_FAMILIA,
	PRODUCTOS.CO_SUBFAMILIA,
	PRODUCTOS.DESC_FAMILIA,
	PRODUCTOS.DESC_SUBFAMILIA,
	B.FIIGCO AS FECHA_CONTRATACION
FROM 
(SELECT *
FROM
STANDARD.IGV0_CONTRATO_P A
WHERE SNAPSHOT = 'LATEST'
AND FIIGCO > date_format(date_sub(current_date(),1), 'yyyy-MM-dd') 
AND XCOEMP = 0 AND FFIGCO IS NULL AND COSIIG = 10000 
AND regexp_replace(A.COPRBS, '^0+','') IN (SELECT * FROM CODIGOS_TITULARES)) B
left join 
(SELECT
COD_PRODUCTO,
CO_FAMILIA,
CO_SUBFAMILIA,
DESC_FAMILIA,
DESC_SUBFAMILIA
FROM
(select
CDFECO AS CO_FAMILIA,
CDSECO AS CO_SUBFAMILIA,
DSFECO AS DESC_FAMILIA,
DSSECO AS DESC_SUBFAMILIA,
COPSER AS COD_PRODUCTO,
ROW_NUMBER() OVER(PARTITION BY COPSER ORDER BY EXTRACTIONDATE DESC) AS RN 
from  STANDARD.MKCPMCDE
where EXTRACTIONDATE >= date_format(date_sub(current_date(),15), 'yyyyMMdd') and ESPECO = 'EN COMERCIALIZACIÓN' AND COESIN = 'A' ) AS PRODUCTOS_15_DIAS
WHERE RN = 1) AS PRODUCTOS
ON B.COPSER = PRODUCTOS.COD_PRODUCTO
LEFT JOIN
(SELECT DISTINCT
                igq_v.coocmk  as valor_alf,
                igq_v.coocda  as valor_num,
                igq_v.noocmk  as descrip_valor
       from  standard.igq_dato igq_d,
                      standard.igq_valor igq_v
        where igq_d.codam1 = igq_v.codamk
              and igq_d.codamk=750
              and igq_d.snapshot = 'LATEST'
              and igq_v.snapshot = 'LATEST') AS DICC_COPSER
			  
ON B.COPSER = DICC_COPSER.valor_alf
;