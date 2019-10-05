with clientes as (
    select 
        case 
            when isnull(coalesce(telefono_movil_1, telefono_movil_2)) then ''
            else coalesce(telefono_movil_1, telefono_movil_2)
        end                                         as tfno_movil,
        case 
            when isnull(ds_email) or ds_email='' then 'NO'
            else ds_email
        end                                          as tenencia_email,
        ds_nombre                                    as nombre,
        edad_cliente                                 as edad_cliente,
        entidad                                      as entidad,
        fc_nacimiento                                as fecha_nacimiento,
        case 
            when isnull(id_canal_preferido) then 0
            else id_canal_preferido
        end                                          as canal_preferido,
        case 
            when id_cartera=700 then 1
            when id_cartera=701 then 1
            else 0                                 
        end                                          as indConectaTuExperto,
        id_cliente                                   as idcliente,
        id_codigo_postal                             as codigopostal,
        id_gestor                                    as idgestor,
         case id_idioma_preferencia  
            when '1' then 'ES'
            when '2' then 'GL'
            when '3' then 'CA'
            when '4' then 'EU'
            when '5' then 'VALENCIANO'
            when '6' then 'EN'
            when '7' then 'DE'
            when '8' then 'FR'
            when '9' then 'PT'
            else 'ES'
        end                                          as idioma_preferencia,
        id_oficina                                   as oficina,
        id_segm_soec                                 as segmento_socieco,
        id_segm_valor_estrategico                    as segmento_estrval,
        id_tipo_persona                              as tipo_persona,
        ind_banca_personal                           as indbancapersonal,
        ind_banca_privada                            as indbancaprivada,
        case 
            when isnull(ind_banca_virtual) then 0 
            else ind_banca_virtual
        end                                          as indbancavirtual,
        id_clasificacion_actividad                   as indclasifactividad,
        case 
            when isnull(coalesce(prefijo_telefono_movil_1,prefijo_telefono_movil_2)) then ''
            when isnotnull(coalesce(prefijo_telefono_movil_1,prefijo_telefono_movil_2)) then '34'
            else substring(coalesce(prefijo_telefono_movil_1,prefijo_telefono_movil_2),3,4)
        end                                          as prefijo_mv,
        case 
            when isnull(telefono_fijo) then ''
            else telefono_fijo                      
        end                                          as tfno_fijo,
        fc_fallecimiento_disolucion                  as fecha_fallecimiento,
        id_sexo                                      as sexo,
        row_number() over(partition by id_cliente order by fecha_derivado desc) as rn
    from DERIVATIVES_GENERIC.BGDV_DMCLIENTES_D
    where DT=201910
        and entidad=0
        and id_tipo_persona=1),
gestores_datos as ( 
    select 
        mkg.cogsmk as codgestor, 
        mki.coemai as emailgestor,
        mkg.codice as codigocentro,
        mkg.cotgmk as tipogestor
    from STANDARD.MKGESTOR as mkg
    join STANDARD.MKGESCOM as mki
    on mkg.COGSMK=mki.COGRMK
    where mkg.SNAPSHOT='LATEST'AND mki.SNAPSHOT='LATEST'),
add_detail_customer01 as (
    select 
        copemk as codCliente,
        in94mk as Precarterizado,
        inpx06 as Colectivo_Valor,
        in16mk as indReservaMapfre
    from standard.mktbblof 
    where SNAPSHOT='LATEST'
        and xcoemp=0
),
add_detail_customer02 as (
    select 
        copemk as CodCliente,
        cogsmk as CodGestor_Valor
     from standard.mksermvl
    where snapshot='LATEST'
            and FFCPMK is null
)

select 
    a.idcliente,
    a.sexo,
    a.prefijo_mv,
    a.tfno_movil,
    a.tfno_fijo,
	a.tenencia_email,
    a.nombre,
    a.edad_cliente,
    a.fecha_nacimiento,
    a.fecha_fallecimiento,
    a.codigopostal,
    a.canal_preferido,
    a.entidad,
	a.idioma_preferencia,
    a.oficina,
    a.segmento_socieco,
    a.segmento_estrval,
    a.tipo_persona,
    a.indConectaTuExperto,
    a.indbancapersonal,
    a.indbancaprivada,
    a.indbancavirtual,
    a.indclasifactividad,
    b.indReservaMapfre,
    coalesce(g.codgestor,0)     as cod_gestor,
    coalesce(g.emailgestor,0)   as mailgestor,
    coalesce(g.codigocentro,0)  as cod_centro,
    coalesce(g.tipogestor,0)    as tipogestor,
    b.Colectivo_Valor,
    b.Precarterizado,
    c.CodGestor_Valor
from clientes as a
left join gestores_datos as g
    on a.idgestor=g.codgestor
    left join add_detail_customer01 as b
        on a.idcliente=b.codCliente
        left join add_detail_customer02 as c
            on a.idcliente=c.CodCliente
where a.rn=1 and a.fecha_fallecimiento is null 