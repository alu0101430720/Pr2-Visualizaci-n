# Pr2-Visualizacion · Pipeline Dagster

## Estructura del proyecto

```
pr2_dagster/
│
├── config.py                  # Constantes globales (rutas, rama, usuario git, fuentes…)
│
├── definitions.py             # Punto de entrada Dagster: assets + jobs + schedules
│
├── assets/                    # Un módulo por capa de datos
│   ├── git_ops.py             #   github_token → clone → configure → pull
│   ├── renta.py               #   ingestar → limpiar → guardar
│   ├── codislas.py            #   ingestar → limpiar → integrar con renta → guardar
│   └── nivelestudios.py       #   ingestar → limpiar → enriquecer → gráficos → commit
│
├── charts/                    # Funciones de visualización (sin Dagster, testeables solas)
│   ├── renta_charts.py        #   graficar_renta, graficar_renta_territorial
│   └── social_charts.py       #   graficar_social
│
└── utils/
    └── git.py                 # Helpers: git_add, git_commit, git_push, commit_and_push
```

> NOTE: Para hacer uso de la lógica del flujo, desde el repositorio hasta los parámetros para graficar, debe usarse el fichero config.py

## DAG de assets

```
github_token ──┬──► clone_repository ──► configure_git ──► pull_repository
               │                                                    │
               │         ┌──────────────────────────────────────────┤
               │         │                                           │
               │    ingestar_renta                        ingestar_codislas
               │         │                                           │
               │    limpiar_renta                         limpiar_codislas
               │         │       \                     /     │
               │  guardar_renta   integrar_renta_codislas    │
               │  _limpia          /          \    guardar_codislas_limpia
               │                 /             \
               │   guardar_renta_integrada  ingestar_nivelestudios
               │                                     │
               │                            limpiar_nivelestudios
               │                                     │
               │                         enriquecer_nivelestudios
               │                            /              \
               │               guardar_nivelestudios  generar_graficos
               │                   _limpio              _ejercicio3
               │                        \               /
               └─────────────────────────► commit_ejercicio3
```

## DAG checks
|Etapa|Nombre del Check|Descripción Técnica|Cómo programarlo (Lógica)|Relación con el Diseño / Gestalt|Información para Metadata|
|:---:|:---:|:---:|:---:|:---:|:---:|
|Carga (Raw)|check_nulos_criticos_renta|Detecta ausencia de datos en variables críticas (Territorio, Tiempo, Medidas y Valores).|df[cols].isna().any(axis=1).sum() == 0|Figura y Fondo: Los huecos inesperados rompen la forma de la visualización.|porcentaje_filas_incompletas, filas_afectadas, detalle_por_columna|
|Carga (Raw)|check_nulos_criticos_codislas|Detecta nulos en ISLA y NOMBRE del catálogo de territorios.|df[['ISLA', 'NOMBRE']].isna().sum() == 0|Figura y Fondo: Los huecos inesperados rompen la forma de la visualización.|nulos_ISLA, nulos_NOMBRE|
|Carga (Raw)|check_nulos_criticos_nivelestudios|Detecta nulos en las columnas clave: Periodo, Sexo, Total.|df[['Periodo', 'Sexo', 'Total']].isna().sum() == 0|Figura y Fondo: Nulos en totales producen áreas apiladas incompletas.|nulos_Periodo, nulos_Sexo, nulos_Total|
|Transf. (Curated)|check_duplicados_limpiar_renta|Verifica que la limpieza no introdujo ni mantuvo filas duplicadas en la renta.|df.duplicated().sum() == 0|Figura y Fondo: Los duplicados inflan artificialmente las series.|filas_duplicadas|
|Transf. (Curated)|check_formato_title_limpiar_renta|Verifica que "Territorio" está en formato Title Case (mayúsculas y minúsculas correctas).|df.str.strip() == df.str.strip().str.title()|Similitud: Mayúsculas inconsistentes duplican las leyendas visuales.|n_incorrectos, ejemplos|
|Transf. (Curated)|check_nombre_invertido_limpiar_renta|Detecta valores con formato 'Apellido, Artículo' (ej. "Gomera, La").|~df.str.contains(",", na=False)|Similitud: Nombres invertidos crean colores distintos para el mismo lugar.|n_invertidos, ejemplos|
|Transf. (Curated)|check_cardinalidad_fuente_renta|Limita el número de fuentes de renta graficadas.|df['Fuente_Renta'].nunique() <= MAX_CATEGORIAS|Carga Cognitiva: Más de 9 colores son imposibles de distinguir visualmente.|n_categorias, limite_recomendado, sugerencia_agrupacion|
|Transf. (Curated)|check_continuidad_serie_temporal_renta|Verifica que no falten años intercalados en la serie temporal.|max(df.Año) - min(df.Año) + 1 == len(df.Año.unique())|Continuidad: Un año faltante crea una pendiente falsa interpolada.|fechas_faltantes, rango_temporal|
|Transf. (Curated)|check_label_text_territorio|Detecta etiquetas de territorio demasiado largas para los ejes.|df['Territorio'].str.len().max() <= MAX_LABEL|Continuidad: Etiquetas demasiado largas se solapan y rompen la legibilidad.|longest_label, longitud_max, overlap_risk|
|Transf. (Curated)|check_cardinalidad_islas|Verifica que el número de islas únicas no supere el límite de colores.|df['ISLA_clean'].nunique() <= MAX_CATEGORIAS|Similitud: Más de 9 colores son imposibles de distinguir visualmente.|n_categorias|
|Transf. (Curated)|check_formato_title_limpiar_codislas|Verifica que ISLA_clean y Territorio estén en Title Case.|_get_title_case_errors() == []|Similitud: Mayúsculas inconsistentes duplican leyendas.|ejemplos_ISLA_clean, ejemplos_Territorio|
|Transf. (Curated)|check_nombre_invertido_limpiar_codislas|Verifica que ISLA_clean y Territorio no contienen formato inverso por coma.|_get_inverted_name_errors() == []|Similitud: Mismo caso que renta, previene duplicidad de entidades.|ejemplos_ISLA_clean, ejemplos_Territorio|
|Transf. (Curated)|check_duplicados_limpiar_codislas|Verifica que no haya municipios duplicados en el catálogo.|df.duplicated().sum() == 0|Figura y Fondo: Un municipio duplicado aparecería dos veces en el facet.|filas_duplicadas, ejemplos|
|Transf. (Curated)|check_integridad_join_renta_codislas|Verifica que todos los municipios cruzados tengan una isla asignada.|df_municipios['ISLA'].isna().sum() == 0|Figura y Fondo: Municipios huérfanos no aparecerán en el facet correcto.|municipios_huerfanos, ejemplos_huerfanos, excluidos_del_check|
|Transf. (Curated)|check_continuidad_serie_temporal_nivelestudios|Verifica que no haya saltos de años en el dataset de nivel de estudios.|max(df.Año) - min(df.Año) + 1 == len(df.Año.unique())|Continuidad: Huecos unen puntos lejanos creando un área engañosa.|fechas_faltantes|
|Transf. (Curated)|check_cardinalidad_nivel_estudios|Limita los distintos niveles de estudios a un máximo de colores.|df['Nivel_estudios'].nunique() <= MAX_CAT|Carga Cognitiva: Demasiadas categorías requieren agruparse (ej. 4 grupos).|n_categorias, sugerencia_agrupacion|
|Transf. (Curated)|check_dominance_otros_nivelestudios|Verifica que el grupo "Sin Estudios/Otros" no domine la visualización.|pct_otros <= DOMINANCE_OTROS_MAX|Semejanza: Un grupo "Otros" dominante atrae atención lejos del dato clave.|pct_of_total, umbral_maximo, dashboard_activo|
|Transf. (Curated)|check_label_text_municipio|Detecta municipios con nombres largos, en específico para heatmaps (eje Y).|df['Municipio'].str.len().max() <= MAX_LABEL|Continuidad: Etiquetas largas se solapan en el eje Y.|longitud_max, overlap_risk|
|Visualiz. (Asset)|check_datos_dashboard_no_vacios|Verifica que los datos filtrados por la configuración del dashboard existan.|len(_filtrar_datos_dashboard(df)) > 0|Figura y Fondo: Un gráfico sin datos no tiene figura que mostrar.|filas_en_grafico, territorios_en_grafico, dashboard_activo|
|Visualiz. (Asset)|check_escala_y_dashboard|Detecta outliers que comprimen los demás datos en el gráfico.|(max / min) <= RATIO_ESCALA_MAX|Proporcionalidad: Evita que barras pequeñas parezcan invisibles ante una gigante.|ratio_escala, valor_outlier, territorio_outlier|
|Visualiz. (Asset)|check_cardinalidad_dashboard|Verifica el número de series (ej. territorios a comparar) en el subset final.|df_subset[col].nunique() <= MAX_CATEGORIAS|Carga Cognitiva: Más de 9 colores son imposibles de distinguir.|n_series, columna_series, dashboard_activo|
|Visualiz. (Asset)|check_orden_magnitud_dashboard|Verifica que las series estén ordenadas descendentemente por valor.|orden_actual == orden_optimo|Continuidad / Prägnanz: El ojo sigue una escalera suave; reduce esfuerzo.|is_sorted, sugerencia_orden, dashboard_activo|
|Visualiz. (Asset)|check_graficos_generados|Verifica que los archivos .png se han guardado físicamente y no están vacíos.|os.path.exists(f) and size > 10.0 KB|Veracidad Visual: Un gráfico vacío o truncado transmite información falsa.|graficos, dashboard_activo|
