 WITH
    DISPARADOR AS (
        SELECT A.copemk AS Persona, B.copemk AS tt, A.copser, A.idprig,
        if(A.copser=11594,1,0) AS cuenta_on,
        if(A.copser=11239,1,0) AS cuenta_joven,
        if(A.copser IN (10300,10600),1,0) AS cuenta_facil,
        if(A.copser IN (10300,10600,11594,11239),1,0) AS total_cuentas
        FROM
        (
          SELECT xcoemp, copser, idprig, copemk, fevaig  FROM standard.igv0_cp_ahca_d tabA
          WHERE 
            xcoemp=0 AND
            feigfi is NULL AND
            tabA.fevaig IN (SELECT max(AA.fevaig) FROM standard.igv0_cp_ahca_d AA) AND
            ffigco is NULL AND
            cosiig=10000 AND
            copser in (10300, 10600, 11594, 11239) AND
            snapshot='LATEST'
          GROUP BY xcoemp, copser, idprig, copemk, fevaig
        ) AS A
        INNER JOIN
        (
          SELECT copemk, xcoemp, ffigco, cosiig, snapshot, coprbs, copser, idprig FROM standard.igv0_contrato_p
          WHERE
            xcoemp=0 AND
            ffigco is NULL AND
            cosiig=10000 AND
            snapshot='LATEST' AND
            coprbs=20
        ) AS B
        ON A.xcoemp=B.xcoemp AND A.copser=B.copser AND A.idprig=B.idprig
        GROUP BY A.copemk, B.copemk, A.copser, A.idprig
      ),
    EXTRACTIONDATE_MKBKSINH AS (
        SELECT MAX(EXTRACTIONDATE) AS E1, MAX(FEVAIG) AS FEV FROM STANDARD.MKBKSINH WHERE trim(clvpro)='HIBD2'
      ),
    DT_BGDV_DMCLIENTES_D AS (
        SELECT MAX(FECHA_DERIVADO) AS FD1 FROM DERIVATIVES_GENERIC.BGDV_DMCLIENTES_D
      )


SELECT Persona AS ID_URSUS, 'JR000002' AS CODIGO_JOURNEY, fecha_nacimiento AS FECHA_NACIMIENTO, saldo_RACs AS IMPORTE_RECURSOS, 
  dom_ingresos_pst AS INDICADOR_INGRESOS_DOMICILIADOS_PST, accionistas_pst AS INDICADOR_ACCIONISTA_PST, racs_pst AS INDICADOR_RACS_PST,
  joven_menos_18_pst AS INDICADOR_JOVEN_MENOR18_PST, joven_18_25_pst AS INDICADOR_JOVEN_1825_PST,

  CASE
    WHEN INDICADOR_DOMICILIACION_INGRESOS_D = 1 THEN 1 ELSE 0
  END AS INDICADOR_DOMICILIACION_INGRESOS_DIARIO,

  CASE
    WHEN PERFIL_DIGITAL = 1 THEN 1 ELSE 0
  END AS INDICADOR_PERFIL_DIGITAL,

  COUNT(distinct X.copser, X.idprig) AS NUMERO_CUENTAS, 
  SUM(cuenta_on) AS NUMERO_CUENTAS_ON,
  SUM(cuenta_joven) AS NUMERO_CUENTAS_JOVEN,
  SUM(X.cuenta_facil) AS NUMERO_CUENTAS_FACIL,

  if(SUM(cuenta_joven+cuenta_on)=sum(total_cuentas),1,0) AS INDICADOR_TODAS_ONJOVEN

FROM DISPARADOR AS X
LEFT JOIN
(
SELECT copemk, fenac9 AS fecha_nacimiento, casfpt/100 AS saldo_RACs, caedad AS edad  FROM standard.mktbblon
WHERE 
   cotpmk=1 AND
   coh004=62120 AND
   snapshot='LATEST' AND DATE_ADD(CURRENT_DATE, 30) = ADD_MONTHS(FENAC9,312)
 ) AS Y
ON X.TT=Y.copemk
LEFT JOIN
(
  SELECT copemk, fevaig, MARIN6 AS dom_ingresos_pst, MARIN3 AS accionistas_pst, MARIN5 AS racs_pst, MARIN1 AS joven_menos_18_pst, MARIN2 AS joven_18_25_pst
  FROM standard.MKBKSINH AS tabZ
  INNER JOIN
    ( 
        SELECT fevaig, copemk
        FROM standard.MKBKSINH AS tabAux
        WHERE tabAux.fevaig IN (SELECT FEV FROM EXTRACTIONDATE_MKBKSINH)
        GROUP BY fevaig, copemk
    ) AS Aux
    ON tabZ.copemk = Aux.copemk AND tabZ.fevaig = Aux.fevaig
  WHERE 
     tabZ.extractiondate IN (SELECT E1 FROM EXTRACTIONDATE_MKBKSINH ) AND
     trim(clvpro)='HIBD2' AND ((MARIN3 = 'S') AND (MARIN5 = 'S') AND (MARIN6 = 'S'))
  GROUP BY tabZ.copemk, tabZ.fevaig, MARIN6, MARIN3, MARIN5, MARIN1, MARIN2
) AS Z
ON X.TT=Z.copemk
LEFT JOIN
(
  SELECT copemk, 1 AS INDICADOR_DOMICILIACION_INGRESOS_D
  FROM STANDARD.IGV0_NOM_CONTCOM_D 
  WHERE SNAPSHOT = 'LATEST' AND xcoemp=0 AND ffigco is NULL AND cosiig=10000
  GROUP BY copemk
) AS U
ON X.TT = U.copemk
LEFT JOIN
(
  SELECT id_cliente, 1 AS PERFIL_DIGITAL
  FROM DERIVATIVES_GENERIC.BGDV_DMCLIENTES_D tabV
  WHERE tabV.FECHA_DERIVADO IN (SELECT FD1 FROM DT_BGDV_DMCLIENTES_D) AND telefono_movil_1 IS NOT NULL AND ds_email = 'SI' AND ind_cr_electronica = 'S' AND ind_disp_moviles = 'S' AND ind_telemarketing = 'S' AND ind_correspondencia_internet = 1
  GROUP BY id_cliente, telefono_movil_1, ds_email, ind_cr_electronica, ind_disp_moviles, ind_telemarketing, ind_correspondencia_internet
) AS V
ON X.TT = V.id_cliente
LEFT JOIN
(
  SELECT copser, idprig
  FROM Disparador Disp
  INNER JOIN
  (
      SELECT copemk, fevaig, MARIN6 AS dom_ingresos, MARIN3 AS accionistas, MARIN5 AS racs, MARIN1 AS joven_menos_18, MARIN2 AS joven_18_25
      FROM standard.MKBKSINH tabE
      INNER JOIN
      ( 
        SELECT fevaig, copemk
        FROM standard.MKBKSINH AS tabAux
        WHERE tabAux.fevaig IN (SELECT FEV FROM EXTRACTIONDATE_MKBKSINH)
        GROUP BY fevaig, copemk
      ) AS Aux
      ON tabE.copemk = Aux.copemk AND tabE.fevaig = Aux.fevaig
      WHERE 
         tabE.extractiondate IN (SELECT E1 FROM EXTRACTIONDATE_MKBKSINH) AND ((MARIN1 = 'N') OR (MARIN3 = 'N') OR (MARIN5 = 'N') OR (MARIN6 = 'N')) AND trim(clvpro)='HIBD2'
      GROUP BY tabE.copemk, tabE.fevaig, MARIN6, MARIN3, MARIN5, MARIN1, MARIN2
  ) AS E
  ON Disp.tt = E.copemk
  GROUP BY copser, idprig
) AS W
ON X.copser = W.copser AND X.idprig = W.idprig
LEFT JOIN
(
  SELECT tt, Disp.cuenta_facil
  FROM Disparador AS Disp
  INNER JOIN
    (
      SELECT id_cliente, telefono_movil_1, ds_email, ind_cr_electronica, ind_disp_moviles, ind_telemarketing, ind_correspondencia_internet
      FROM DERIVATIVES_GENERIC.BGDV_DMCLIENTES_D tabV
      WHERE tabV.FECHA_DERIVADO IN (SELECT FD1 FROM DT_BGDV_DMCLIENTES_D) AND telefono_movil_1 IS NOT NULL AND ds_email = 'SI' AND ind_cr_electronica = 'S' AND ind_disp_moviles = 'S' AND ind_telemarketing = 'S' AND ind_correspondencia_internet = 1
      GROUP BY id_cliente, telefono_movil_1, ds_email, ind_cr_electronica, ind_disp_moviles, ind_telemarketing, ind_correspondencia_internet
    ) AS F
    ON Disp.tt = F.id_cliente
  WHERE Disp.cuenta_facil = 0
  GROUP BY tt, copser, cuenta_facil
) AS T
ON X.TT = T.tt
WHERE W.copser IS NULL AND W.idprig IS NULL 
AND T.tt IS NULL
AND Y.copemk IS NOT NULL  --eliminamos las cuentas de las personas jurídicas, también las de pfs nuevas
GROUP BY Persona,fecha_nacimiento,saldo_RACs,edad, dom_ingresos_pst, accionistas_pst, racs_pst, joven_menos_18_pst, joven_18_25_pst, INDICADOR_DOMICILIACION_INGRESOS_D, PERFIL_DIGITAL
