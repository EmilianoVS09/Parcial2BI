'''
Justificación de las transformaciones (en función de las preguntas de negocio)

Este pipeline prepara y enriquece tres colecciones de Airbnb (listings, calendar y reviews) para poder responder, con datos consistentes y comparables, a preguntas como: la relación entre verificaciones y calificaciones del anfitrión, la localización y temporada alta/ baja, el vínculo precio–ocupación en el tiempo, el impacto de las tasas de respuesta/aceptación, el efecto de ser superhost, la actividad reciente de reviews, la limpieza/comunicación, la ocupación anual y la variación de precio, entre otras. A continuación se explica, paso a paso, por qué se aplica cada transformación.

1) Normalización de tipos (fechas, precios, porcentajes, texto) y descarte de columnas de ruido

Fechas → ISO (YYYY-MM-DD) en listings, calendar y reviews: necesitamos una base temporal homogénea para detectar temporada alta/baja y comparar métricas por mes/quarter/fin de semana (p. ej., ¿qué colonias concentran mejores calificaciones en temporada alta? y ¿cómo varía la ocupación a lo largo del tiempo?). También habilita ventanas móviles como “últimos 30 días” en reseñas.

Precios → numérico (price_num): imprescindible para estimar ingresos y estudiar la relación precio–ocupación en series diarias y por segmentos (¿cómo varía el precio en temporada baja?, ¿existe correlación entre calificaciones altas y precios más elevados?).

Porcentajes → numérico (host_response_rate_pct, host_acceptance_rate_pct): permite medir la influencia de la tasa de aceptación/ respuesta del anfitrión en la ocupación sin sesgos por formatos de texto.

Limpieza de texto (remover HTML, normalizar Unicode): necesario para segmentar correctamente por colonia/barrio y tipo de habitación, y para reportes interpretables cuando comparamos ubicación, amenities o atributos del host con calificación y ocupación.

Eliminar columnas de calendar no usadas (adjusted_price, _id): reduce ruido que podría distorsionar métricas de precio diario y ocupación.

2) Limpieza de nulos e imputaciones jerárquicas (con banderas de faltantes)

Protección de claves (id, host_id) y tipificación numérica previa: asegura joins correctos y cálculos confiables, evitando pérdidas de muestra que sesgarían resultados al comparar colonias o periodos (impacto directo sobre precio–ocupación, ocupación anual y temporada alta/baja).

Estandarización de NA en textos y rellenos básicos (p. ej., host_name, host_response_time): evita categorías fantasma y mantiene la posibilidad de analizar el rol del host (p. ej., ser superhost o su tiempo de respuesta) frente a ocupación y calificaciones.

Banderas _was_missing: dejan trazabilidad de dónde se imputó, clave para auditar si una relación (p. ej., verificación ↔ calificación o precio ↔ ocupación) podría estar influida por datos faltantes.

Imputación de tasas del host (respuesta/aceptación): en cascada host → grupo (barrio/tipo/aforo) → global y recorte a [0,100]. Esta estrategia mantiene comparabilidad entre listings para analizar cómo influyen estas tasas en la ocupación sin inflar/deflactar extremos.

Imputación de review_scores_*: por grupo y barrio, luego global, y winsorization p1–p99. Esto estabiliza las calificaciones (limpieza, comunicación, etc.) frente a outliers, permitiendo evaluar de forma robusta:

¿La verificación del anfitrión se asocia a mejores calificaciones?

¿Ser superhost impacta calificación y ocupación?

¿Existe correlación entre calificaciones altas y precios más elevados?

reviews_per_month → 0.0: evita perder propiedades en análisis de actividad reciente (top reseñas últimos 30 días) y su relación con ocupación e ingresos.

Coordenadas (latitude/longitude) imputadas por barrio y global: imprescindibles para analizar localización ↔ precio promedio, comparar colonias y detectar tendencias por zona (ocupación alta/ baja).

Contadores y disponibilidades (availability, número de reseñas, accommodates, etc.) imputados por grupo y global, y convertidos a enteros ≥ 0: aseguran que medidas como ocupación anual y disponibilidad en temporada baja sean comparables entre propiedades.

Booleanos y categóricos (superhost, verificación de identidad, instant bookable, room_type, neighbourhood_cleansed): rellenados de forma consistente para habilitar cortes como superhost vs no superhost en ocupación y calificaciones, o tipos de habitación y colonias frente a precio y ocupación.

host_since con fallback (last_scraped → fecha fija): permite usar antigüedad del host como factor explicativo en ocupación y calificación, evitando huecos que invaliden comparaciones.

amenities y host_verifications forzados a listas: precondición para crear dummies controlados. Sin esto no podríamos estimar el aporte de amenities clave a ocupación ni la relación verificación ↔ calificación del anfitrión.

Calendar: available normalizado (t/f → bool) y noches mín/max: available es el núcleo del cálculo de ocupación (y por extensión ingresos). La normalización asegura métricas temporales confiables; las noches mín/max evitan valores erráticos en temporadas concretas.

Reviews: drops por campos críticos y rellenos base: garantiza que conteos por mes (y “últimos 30 días”) sean coherentes, fundamentales para evaluar si más reseñas recientes se asocian a mayor ocupación e ingresos.

Red de seguridad (fill-all): completa nulos residuales por tipo para que ningún análisis temático (colonias, precio–ocupación, superhost, verificación, limpieza/comunicación) quede invalidadо por NaNs.

3) Derivación de rasgos temporales y buckets de precio

Partes de fecha (año, mes, quarter, día de semana, fin de semana): habilitan cortes inteligentes para temporada alta/baja, patrones por fin de semana, y análisis longitudinal precio–ocupación (¿qué colonias concentran mejores calificaciones en temporada alta? y ¿cómo varía el precio/ocupación durante el año?).

Buckets de precio (quintiles o bins fijos) en listings y calendar: estratifican el mercado para comparar ocupación e ingresos por nivel de precio, y estudiar si calificaciones altas se asocian a precios más altos sin que unos pocos valores extremos dominen el análisis.

4) Expansión controlada de campos anidados (amenities y verificaciones)

Amenidades seleccionadas

No se incluyen todas las amenidades del dataset, sino un conjunto curado de aquellas que inciden directamente en la experiencia del huésped y, por tanto, en calificaciones, ocupación e ingresos.

Conectividad y trabajo: Wifi, Dedicated workspace
→ La presencia de wifi y espacios de trabajo influye en la ocupación de largo plazo y en la preferencia de viajeros de negocios.

Comodidad y autoservicio: Self check-in, Free parking, Kitchen, Refrigerator, Microwave, Stove, Coffee maker, Cooking basics, Dishes and silverware
→ Estas amenidades facilitan la autonomía del huésped y suelen correlacionarse con calificaciones más altas y mayor ocupación, especialmente en estancias familiares o prolongadas.

Confort y descanso: Room-darkening shades, Essentials, Washer
→ Representan condiciones básicas que impactan la satisfacción y, por tanto, las reseñas de limpieza y comodidad.

Seguridad y confianza: Smoke alarm, Carbon monoxide alarm, Fire extinguisher, First aid kit
→ Son determinantes en la percepción de seguridad, que puede reflejarse en mejores puntuaciones de valor y limpieza.

La selección equilibra tres ejes: usabilidad (comodidades funcionales), seguridad y valor percibido, alineados con las preguntas sobre ocupación, calificación y precios.
Reducir el set a estos ítems evita dimensionalidad excesiva y mantiene solo aquellos con valor interpretativo en los análisis.

Dummies de host_verifications (teléfono, email, work_email): instrumentan de manera directa la pregunta ¿qué relación existe entre la verificación del anfitrión y la calificación promedio? y permiten controlar por este factor cuando se estudia ocupación.

5) Imputación del precio de listing con evidencia operativa y recalculo de buckets

listing_price_num imputado con: mediana de calendar por listing → mediana por barrio → mediana global. Se prioriza el precio observado (calendar) frente al de catálogo para contestar con mayor fidelidad ¿cuál es la relación entre el precio y la ocupación a lo largo del tiempo? y evaluar variaciones en temporada baja. El paso por barrio captura gradientes espaciales del precio promedio.

Recalcular price_bucket tras imputar: crucial para que comparaciones por estrato de precio (ocupación, calificación, ingresos) no queden sesgadas por nulos o por cambios en la distribución.

6) Construcción de la “sábana” diaria (flat sheet)

Por qué se escogieron esos campos para la sábana

Los campos seleccionados en base_keep representan los mínimos necesarios para explicar la ocupación y el desempeño de cada propiedad desde tres dimensiones complementarias:

Características del inmueble: tipo, tamaño, número de camas y baños, amenities, etc.
→ Permiten estudiar cómo los atributos físicos y funcionales influyen en el precio, las calificaciones y la demanda.

Características del anfitrión: tasas de respuesta y aceptación, superhost, verificaciones, experiencia (host_since).
→ Permiten analizar cómo la gestión del anfitrión impacta la ocupación y la percepción del huésped.

Rendimiento operativo: precio, disponibilidad, reseñas, ocupación e ingresos.
→ Son las variables de salida que se explican a partir de las anteriores.

En conjunto, esta estructura integra la oferta (propiedad y host) con la demanda (ocupación, reseñas, ingresos), creando un dataset coherente para todas las preguntas de negocio.
El resultado es una sábana que combina contexto temporal, geográfico y comportamental, apta para análisis exploratorios, modelos predictivos y tableros de inteligencia.

Medidas diarias:

booked_night = ~available → ocupación a nivel de fecha-propiedad;

daily_revenue = booked_night * price_num → ingresos diarios.
Estas dos métricas permiten responder de forma directa: precio–ocupación en el tiempo, ocupación anual, propiedades con mayor ocupación, cómo varía el precio en temporada baja, y cómo influyen reseñas recientes en ocupación e ingresos.

Join calendar ← atributos de listings (tipos de ID alineados): vincula la dinámica diaria (ocupación/precio) con el perfil de la propiedad y del host (superhost, verificaciones, tasas de respuesta/aceptación, amenities), habilitando análisis causales/explicativos.

reviews_in_month (conteo por listing/mes): proxy de actividad reciente de huéspedes; responde ¿qué propiedades tienen más reseñas en L30D y cómo influye en su ocupación e ingresos?

Garantías de no-nulos en campos críticos (fechas de review, listing_price_num, price_bucket, reviews_in_month): aseguran que ninguna propiedad quede fuera de comparaciones clave (colonias, precio–ocupación, superhost, verificaciones, limpieza/comunicación, disponibilidad en temporada baja).

Orden de columnas (fechas y métricas diarias al frente): pensado para facilitar agregaciones y dashboards que responden rápidamente a todas las preguntas (por ejemplo, agrupar por colonia y quarter para detectar temporada alta y cruzar con calificaciones o ocupación).

7) Logging integral

Se registran transformaciones aplicadas, tamaños antes/después, y cuentas de imputación por estrategia/fuente (host, grupo, barrio, global; reviews vs calendar para fechas; calendar/barrio/global para precio). Esto permite auditar si las conclusiones (p. ej., verificación ↔ calificación, superhost ↔ ocupación/calificación, localización ↔ precio, precio ↔ ocupación) podrían estar influidas por la limpieza y dónde.
'''

import pandas as pd
import numpy as np
import unicodedata
import re
from logs import Logs

class Transformation:
    # ---------------------------------------------------------------------
    # Constructor
    # ---------------------------------------------------------------------
    def __init__(self, df_listings: pd.DataFrame, df_calendar: pd.DataFrame, df_reviews: pd.DataFrame):
        """
        Propósito:
          - Inicializar copias de dataframes y el manejador de logs.

        Logs:
          - Tamaño y cantidad de columnas de cada dataframe.

        Retorna:
          - self (vía métodos encadenables).
        """
        self.logs = Logs()
        self.listings = df_listings.copy()
        self.calendar = df_calendar.copy()
        self.reviews  = df_reviews.copy()
        self.flat_sheet = None

        self.logs.log(
            f"[INIT] Recibidos | "
            f"listings: {self.listings.shape} cols={len(self.listings.columns)} | "
            f"calendar: {self.calendar.shape} cols={len(self.calendar.columns)} | "
            f"reviews: {self.reviews.shape} cols={len(self.reviews.columns)}",
            "info"
        )

    # ------------------------ Helpers de normalización --------------------

    def _parse_any_date(self, v):
        """Convierte a datetime UTC valores string/dict/$date; NaT si falla."""
        if pd.isna(v): return pd.NaT
        if isinstance(v, dict) and '$date' in v: v = v['$date']
        return pd.to_datetime(v, errors='coerce', utc=True)

    def _to_iso_date(self, s: pd.Series) -> pd.Series:
        """Serie → 'YYYY-MM-DD' (string ISO de fecha)."""
        return s.apply(self._parse_any_date).dt.strftime('%Y-%m-%d')

    def _to_iso_datetime(self, s: pd.Series) -> pd.Series:
        """Serie → 'YYYY-MM-DDTHH:MM:SSZ' (string ISO datetime)."""
        return s.apply(self._parse_any_date).dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    def _to_price_num(self, s: pd.Series) -> pd.Series:
        """Limpia '$' y comas → float."""
        return (s.astype(str).str.replace(r'[\$,]', '', regex=True)
                             .replace({'': np.nan}).astype(float))

    def _to_percent_num(self, s: pd.Series) -> pd.Series:
        """Elimina '%' y 'N/A' → float."""
        return (s.astype(str).str.replace('%', '', regex=False)
                             .replace({'': np.nan, 'N/A': np.nan}).astype(float))

    def _clean_text(self, s: pd.Series) -> pd.Series:
        """Quita HTML, normaliza Unicode, colapsa espacios, limpia 'nan'."""
        t = s.astype(str)
        t = t.str.replace(r'<br\s*/?>', ' ', flags=re.I, regex=True)
        t = t.str.replace(r'<[^>]+>', ' ', regex=True)
        t = t.apply(lambda x: unicodedata.normalize('NFC', x))
        t = t.str.replace(r'\s+', ' ', regex=True).str.strip()
        return t.replace({'None':'','nan':'','NaN':''})

    def _shape_str(self):
        """Devuelve resumen de formas de dataframes para logging."""
        return (f"listings={self.listings.shape} | "
                f"calendar={self.calendar.shape} | "
                f"reviews={self.reviews.shape}")

    # ---------------------------------------------------------------------
    # 1) Normalizar tipos
    # ---------------------------------------------------------------------
    def normalize_types(self):
        """
        Propósito:
          - Unificar tipos: fechas (→ ISO string), precios (→ float), % (→ float),
            textos (→ limpio) y dropear columnas no usadas.

        Transformaciones:
          - listings: fechas (last_scraped, host_since, first/last_review) a ISO.
          - calendar/reviews: date a ISO.
          - price → price_num (float) y se elimina price.
          - host_*_rate → *_pct (float) y se elimina original.
          - limpieza de texto en columnas descriptivas.
          - relleno de first_review/last_review con fuentes jerárquicas.

        Logs:
          - Inicio/fin, columnas eliminadas, columnas de % creadas.

        Retorna:
          - self
        """
        self.logs.log("[normalize_types] Inicio", "info")

        # Fechas -> ISO (strings)
        for c in ['last_scraped','calendar_last_scraped','host_since','first_review','last_review']:
            if c in self.listings.columns:
                self.listings[c] = self._to_iso_date(self.listings[c])
        if 'date' in self.calendar.columns:
            self.calendar['date'] = self._to_iso_date(self.calendar['date'])
        if 'date' in self.reviews.columns:
            self.reviews['date'] = self._to_iso_date(self.reviews['date'])

        # Calendar: columnas no usadas
        dropped = []
        for c in ['adjusted_price','_id']:
            if c in self.calendar.columns:
                self.calendar.drop(columns=[c], inplace=True, errors='ignore')
                dropped.append(c)
        if dropped:
            self.logs.log(f"[normalize_types] Calendar: drop {dropped}", "info")

        # Precios → numérico y se elimina 'price'
        if 'price' in self.listings.columns and 'price_num' not in self.listings.columns:
            self.listings['price_num'] = self._to_price_num(self.listings['price'])
        if 'price' in self.listings.columns:
            self.listings.drop(columns=['price'], inplace=True, errors='ignore')

        # % → numérico y se elimina original
        pct_new = []
        for c in ['host_response_rate','host_acceptance_rate']:
            if c in self.listings.columns:
                newc = c + '_pct'
                self.listings[newc] = self._to_percent_num(self.listings[c])
                self.listings.drop(columns=[c], inplace=True, errors='ignore')
                pct_new.append(newc)
        if pct_new:
            self.logs.log(f"[normalize_types] Listings: % → {pct_new}", "info")

        # Calendar: precio → numérico
        if 'price' in self.calendar.columns and 'price_num' not in self.calendar.columns:
            self.calendar['price_num'] = self._to_price_num(self.calendar['price'])
        if 'price' in self.calendar.columns:
            self.calendar.drop(columns=['price'], inplace=True, errors='ignore')

        # Limpieza de texto
        text_cols = ['name','description','neighborhood_overview','neighbourhood','neighbourhood_cleansed',
                     'property_type','room_type','host_name','host_location','host_neighbourhood','host_response_time']
        touched = [c for c in text_cols if c in self.listings.columns]
        for c in touched:
            self.listings[c] = self._clean_text(self.listings[c])
        if 'reviewer_name' in self.reviews.columns:
            self.reviews['reviewer_name'] = self._clean_text(self.reviews['reviewer_name'])
        if 'comments' in self.reviews.columns:
            self.reviews['comments'] = self._clean_text(self.reviews['comments'])
        if touched:
            self.logs.log(f"[normalize_types] Limpieza de texto: {len(touched)} cols", "info")

        # Completar fechas de review con jerarquía
        self._fill_review_dates()

        self.logs.log(f"[normalize_types] Fin | {self._shape_str()}", "info")
        return self

    # ---------------------------------------------------------------------
    # 2) Nulos e imputaciones
    # ---------------------------------------------------------------------
    def clean_nulls(self):
        """
        Propósito:
          - Imputar faltantes con estrategia jerárquica y red de seguridad.
          - Normalizar NA, booleanos y categóricos.
          - Tipificar numéricos y discretizar contadores.

        Transformaciones principales:
          - Drops por claves (id/host_id).
          - Flags *_was_missing para auditoría.
          - Tasas host_*_pct: host → grupo → global → clip 0..100.
          - Scores: grupo/barrio → global → winsor p1..p99.
          - reviews_per_month: NaN → 0.0.
          - Lat/Lon: barrio → global.
          - Contadores: grupo → global → enteros ≥ 0.
          - Booleanos y categóricos: NaN → False/mode/'unknown'.
          - host_since: last_scraped → '1970-01-01T00:00:00Z'.
          - amenities/host_verifications: forzar listas.
          - calendar.available: normalizar 't'/'f' y NaN → True.
          - minimum/maximum_nights: NaN → mediana.
          - reviews: drop por críticos; rellenar reviewer_name/comments.
          - Red de seguridad: completar cualquier NaN residual por tipo.

        Logs:
          - Filas eliminadas por claves.
          - Conteo por estrategia de imputación en tasas, scores, coords, contadores.
          - Normalización de calendar.available y mínimos/máximos.
          - Limpieza universal aplicada por columna.

        Retorna:
          - self
        """
        self.logs.log("[clean_nulls] Inicio", "info")
        before_l = len(self.listings)
        before_c = len(self.calendar)
        before_r = len(self.reviews)

        # --- Protege claves obligatorias ---
        drop_counts = {}
        if 'id' in self.listings.columns:
            b = len(self.listings)
            self.listings = self.listings.dropna(subset=['id'])
            drop_counts['id'] = b - len(self.listings)
        if 'host_id' in self.listings.columns:
            b = len(self.listings)
            self.listings = self.listings.dropna(subset=['host_id'])
            drop_counts['host_id'] = drop_counts.get('host_id', 0) + (b - len(self.listings))
        if drop_counts:
            self.logs.log(f"[clean_nulls] Drops por claves: {drop_counts}", "warning")

        # --- Tipos numéricos esperados ---
        score_cols = [c for c in [
            'review_scores_rating','review_scores_cleanliness','review_scores_accuracy',
            'review_scores_communication','review_scores_checkin','review_scores_location','review_scores_value'
        ] if c in self.listings.columns]
        rate_cols = [c for c in ['host_response_rate_pct','host_acceptance_rate_pct'] if c in self.listings.columns]
        count_cols = [c for c in [
            'availability_30','availability_60','availability_90','availability_365',
            'number_of_reviews','number_of_reviews_ltm','number_of_reviews_l30d','number_of_reviews_ly',
            'accommodates','bedrooms','bathrooms','beds','host_total_listings_count'
        ] if c in self.listings.columns]
        for c in score_cols + rate_cols + count_cols + ['price_num','reviews_per_month','latitude','longitude']:
            if c in self.listings.columns:
                self.listings[c] = pd.to_numeric(self.listings[c], errors='coerce')

        # --- NA estandarizados y textos base ---
        na_like = {'N/A': np.nan, 'n/a': np.nan, 'NA': np.nan, 'na': np.nan, 'None': np.nan, '': np.nan}
        self.listings.replace(na_like, inplace=True)
        if 'host_name' in self.listings.columns:
            self.listings['host_name'] = self.listings['host_name'].fillna('unknown host')
        if 'host_response_time' in self.listings.columns:
            self.listings['host_response_time'] = self.listings['host_response_time'].fillna('unknown')

        # --- Banderas de faltantes antes de imputar ---
        for c in score_cols + rate_cols + ['latitude','longitude','reviews_per_month']:
            if c in self.listings.columns:
                self.listings[f'{c}_was_missing'] = self.listings[c].isna().astype(int)

        # --- Claves de agrupación para imputar ---
        group_keys = [k for k in ['neighbourhood_cleansed','room_type','accommodates'] if k in self.listings.columns]

        # --- Tasas del host: host → grupo → global → clip ---
        if 'host_id' in self.listings.columns:
            for c in rate_cols:
                filled_host = filled_grp = 0
                m_init = self.listings[c].isna().sum()
                by_host = self.listings.groupby('host_id')[c].median(numeric_only=True)
                m = self.listings[c].isna()
                if not by_host.empty:
                    before_na = m.sum()
                    self.listings.loc[m, c] = self.listings.loc[m, 'host_id'].map(by_host)
                    filled_host = before_na - self.listings[c].isna().sum()
                if group_keys:
                    by_grp = self.listings.groupby(group_keys)[c].median(numeric_only=True)
                    m = self.listings[c].isna()
                    if not by_grp.empty and m.any():
                        before_na = m.sum()
                        self.listings.loc[m, c] = self.listings.loc[m, group_keys].apply(
                            lambda r: by_grp.get(tuple(r.values), np.nan), axis=1
                        )
                        filled_grp = before_na - self.listings[c].isna().sum()
                m = self.listings[c].isna()
                glob = self.listings[c].median()
                filled_glob = m.sum()
                self.listings[c] = self.listings[c].fillna(glob).clip(0, 100)
                self.logs.log(
                    f"[clean_nulls] {c}: host={filled_host} grupo={filled_grp} global={filled_glob}",
                    "info"
                )

        # --- Scores: grupo/barrio → global → winsor p1..p99 ---
        for c in score_cols:
            filled_grp = filled_nb = 0
            if group_keys:
                by_grp = self.listings.groupby(group_keys)[c].median(numeric_only=True)
                m = self.listings[c].isna()
                if not by_grp.empty and m.any():
                    b = m.sum()
                    self.listings.loc[m, c] = self.listings.loc[m, group_keys].apply(
                        lambda r: by_grp.get(tuple(r.values), np.nan), axis=1
                    )
                    filled_grp = b - self.listings[c].isna().sum()
            if 'neighbourhood_cleansed' in self.listings.columns:
                by_nb = self.listings.groupby('neighbourhood_cleansed')[c].median(numeric_only=True)
                m = self.listings[c].isna()
                if not by_nb.empty and m.any():
                    b = m.sum()
                    self.listings.loc[m, c] = self.listings.loc[m, 'neighbourhood_cleansed'].map(by_nb)
                    filled_nb = b - self.listings[c].isna().sum()
            m = self.listings[c].isna()
            glob = self.listings[c].median()
            filled_glob = m.sum()
            self.listings[c] = self.listings[c].fillna(glob)
            v = pd.to_numeric(self.listings[c], errors='coerce')
            lo, hi = v.quantile(0.01), v.quantile(0.99)
            self.listings[c] = v.clip(lower=lo if np.isfinite(lo) else v.min(),
                                      upper=hi if np.isfinite(hi) else v.max())
            self.logs.log(
                f"[clean_nulls] {c}: grupo={filled_grp} barrio={filled_nb} global={filled_glob} winsor p1..p99",
                "info"
            )

        # --- Reviews/mes: NaN → 0.0 ---
        if 'reviews_per_month' in self.listings.columns:
            n_na = self.listings['reviews_per_month'].isna().sum()
            self.listings['reviews_per_month'] = self.listings['reviews_per_month'].fillna(0.0)
            if n_na:
                self.logs.log(f"[clean_nulls] reviews_per_month: {n_na} → 0.0", "info")

        # --- Coordenadas: barrio → global ---
        for c in ['latitude','longitude']:
            if c in self.listings.columns:
                filled_nb = 0
                if 'neighbourhood_cleansed' in self.listings.columns:
                    by_nb = self.listings.groupby('neighbourhood_cleansed')[c].median(numeric_only=True)
                    m = self.listings[c].isna()
                    if not by_nb.empty and m.any():
                        b = m.sum()
                        self.listings.loc[m, c] = self.listings.loc[m, 'neighbourhood_cleansed'].map(by_nb)
                        filled_nb = b - self.listings[c].isna().sum()
                m = self.listings[c].isna()
                glob = self.listings[c].median()
                filled_glob = m.sum()
                self.listings[c] = self.listings[c].fillna(glob)
                self.logs.log(f"[clean_nulls] {c}: barrio={filled_nb} global={filled_glob}", "info")

        # --- Contadores: grupo → global → enteros ≥ 0 ---
        for c in count_cols:
            filled_grp = 0
            if group_keys:
                by_grp = self.listings.groupby(group_keys)[c].median(numeric_only=True)
                m = self.listings[c].isna()
                if not by_grp.empty and m.any():
                    b = m.sum()
                    self.listings.loc[m, c] = self.listings.loc[m, group_keys].apply(
                        lambda r: by_grp.get(tuple(r.values), np.nan), axis=1
                    )
                    filled_grp = b - self.listings[c].isna().sum()
            m = self.listings[c].isna()
            glob = self.listings[c].median()
            filled_glob = m.sum()
            self.listings[c] = self.listings[c].fillna(glob)
            self.listings[c] = np.maximum(0, np.floor(pd.to_numeric(self.listings[c], errors='coerce'))).astype(int)
            self.logs.log(f"[clean_nulls] {c}: grupo={filled_grp} global={filled_glob} cast→int≥0", "info")

        # --- Booleanos y categóricos ---
        for c in ['host_is_superhost','host_has_profile_pic','host_identity_verified','instant_bookable']:
            if c in self.listings.columns:
                n_na = self.listings[c].isna().sum()
                self.listings[c] = self.listings[c].fillna(False)
                if n_na:
                    self.logs.log(f"[clean_nulls] {c}: {n_na} → False", "info")
        for c in ['property_type','room_type','neighbourhood_cleansed']:
            if c in self.listings.columns:
                mode = self.listings[c].mode(dropna=True)
                n_na = self.listings[c].isna().sum()
                self.listings[c] = self.listings[c].fillna(mode.iloc[0] if not mode.empty else 'unknown')
                if n_na:
                    self.logs.log(f"[clean_nulls] {c}: {n_na} → mode/unknown", "info")

        # --- host_since: fallback en cascada ---
        if 'host_since' in self.listings.columns:
            mask = self.listings['host_since'].isna() | (self.listings['host_since'].astype(str).str.strip() == '')
            n_mask = mask.sum()
            if 'last_scraped' in self.listings.columns and n_mask:
                self.listings.loc[mask, 'host_since'] = self.listings.loc[mask, 'last_scraped']
                mask = self.listings['host_since'].isna() | (self.listings['host_since'].astype(str).str.strip() == '')
            n_final = mask.sum()
            if n_final:
                self.listings.loc[mask, 'host_since'] = '1970-01-01T00:00:00Z'
            self.logs.log(f"[clean_nulls] host_since: desde last_scraped={n_mask - n_final} | a 1970={n_final}", "info")

        # --- Forzar listas en campos anidados ---
        for c in ['amenities','host_verifications']:
            if c in self.listings.columns:
                def as_list(x):
                    if isinstance(x, list): return x
                    import ast
                    if isinstance(x, str) and x.strip().startswith('['):
                        try: return ast.literal_eval(x)
                        except: return []
                    if pd.isna(x) or x == '': return []
                    return [str(x)]
                self.listings[c] = self.listings[c].apply(as_list)

        # --- Calendar: available y noches mín/max ---
        if 'available' in self.calendar.columns:
            n_na = self.calendar['available'].isna().sum()
            self.calendar['available'] = self.calendar['available'].map({'t': True, 'f': False, True: True, False: False})
            self.calendar['available'] = self.calendar['available'].fillna(True)
            if n_na:
                self.logs.log(f"[clean_nulls] calendar.available: {n_na} → True (normalizado)", "info")
        for c in ['minimum_nights','maximum_nights']:
            if c in self.calendar.columns:
                med = pd.to_numeric(self.calendar[c], errors='coerce').median()
                n_na = self.calendar[c].isna().sum()
                self.calendar[c] = pd.to_numeric(self.calendar[c], errors='coerce').fillna(med)
                if n_na:
                    self.logs.log(f"[clean_nulls] calendar.{c}: {n_na} → mediana={med}", "info")
        if 'price_num' in self.calendar.columns:
            self.calendar['price_num'] = pd.to_numeric(self.calendar['price_num'], errors='coerce')

        # --- Reviews: drops por críticos y rellenos base ---
        before_rev = len(self.reviews)
        crit = [c for c in ['listing_id','id','date'] if c in self.reviews.columns]
        if crit:
            self.reviews = self.reviews.dropna(subset=crit)
            dropped = before_rev - len(self.reviews)
            if dropped:
                self.logs.log(f"[clean_nulls] Reviews: drops por {crit} = {dropped}", "warning")
        if 'reviewer_name' in self.reviews.columns:
            n_na = self.reviews['reviewer_name'].isna().sum()
            self.reviews['reviewer_name'] = self.reviews['reviewer_name'].fillna('unknown')
            if n_na:
                self.logs.log(f"[clean_nulls] reviewer_name: {n_na} → 'unknown'", "info")
        if 'comments' in self.reviews.columns:
            n_na = self.reviews['comments'].isna().sum()
            self.reviews['comments'] = self.reviews['comments'].fillna('')
            if n_na:
                self.logs.log(f"[clean_nulls] comments: {n_na} → ''", "info")

        # --- Red de seguridad: completa cualquier NaN residual por tipo ---
        def fill_all_nulls(df, name):
            changed = {}
            for c in df.columns:
                if df[c].isna().any():
                    n_na = df[c].isna().sum()
                    if pd.api.types.is_numeric_dtype(df[c]):
                        med = pd.to_numeric(df[c], errors='coerce').median()
                        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(med)
                        changed[c] = f"num→mediana({med}) {n_na}"
                    elif pd.api.types.is_bool_dtype(df[c]):
                        df[c] = df[c].fillna(False)
                        changed[c] = f"bool→False {n_na}"
                    elif pd.api.types.is_datetime64_any_dtype(df[c]):
                        df[c] = df[c].fillna(pd.Timestamp('1970-01-01T00:00:00Z'))
                        changed[c] = f"datetime→1970 {n_na}"
                    elif df[c].apply(lambda x: isinstance(x, list)).any():
                        df[c] = df[c].apply(lambda x: [] if pd.isna(x) else x)
                        changed[c] = f"list→[] {n_na}"
                    else:
                        df[c] = df[c].astype(object).fillna('unknown')
                        changed[c] = f"str→'unknown' {n_na}"
            if changed:
                self.logs.log(f"[clean_nulls] Red de seguridad {name}: {changed}", "info")
            return df

        self.listings = fill_all_nulls(self.listings, "listings")
        self.calendar = fill_all_nulls(self.calendar, "calendar")
        self.reviews  = fill_all_nulls(self.reviews,  "reviews")

        # --- Resumen final de limpieza ---
        self.logs.log(
            f"[clean_nulls] Fin | tamaños antes L:{before_l} C:{before_c} R:{before_r} | "
            f"después L:{len(self.listings)} C:{len(self.calendar)} R:{len(self.reviews)}",
            "info"
        )
        return self

    # ---------------------------------------------------------------------
    # 3) Rasgos de fecha y buckets de precio
    # ---------------------------------------------------------------------
    def derive_features(self, price_mode='quantile', price_bins=None, price_labels=None):
        """
        Propósito:
          - Crear partes de fecha en calendar y buckets de precio en listings/calendar.

        Transformaciones:
          - calendar.date → year, month, day, quarter, weekday, is_weekend.
          - price_num → price_bucket (listings) / daily_price_bucket (calendar)
            usando quintiles por defecto o bins fijos.

        Logs:
          - Inicio/fin y avisos si qcut/cut falla.

        Retorna:
          - self
        """
        self.logs.log(f"[derive_features] Inicio | price_mode={price_mode}", "info")

        # Partes de fecha
        if 'date' in self.calendar.columns:
            dt = pd.to_datetime(self.calendar['date'], errors='coerce', utc=True)
            self.calendar['year']       = dt.dt.year
            self.calendar['month']      = dt.dt.month
            self.calendar['day']        = dt.dt.day
            self.calendar['quarter']    = dt.dt.quarter
            self.calendar['weekday']    = dt.dt.weekday
            self.calendar['is_weekend'] = self.calendar['weekday'].isin([5,6]).astype(int)

        # Buckets de precio
        default_labels = ['Very Low','Low','Medium','High','Very High']
        if 'price_num' in self.listings.columns:
            s = pd.to_numeric(self.listings['price_num'], errors='coerce')
            try:
                if price_mode == 'quantile':
                    self.listings['price_bucket'] = pd.qcut(s, q=5, labels=price_labels or default_labels, duplicates='drop')
                elif price_mode == 'fixed' and price_bins is not None:
                    self.listings['price_bucket'] = pd.cut(s, bins=price_bins, labels=price_labels, include_lowest=True)
            except Exception as e:
                self.logs.log(f"[derive_features] price_bucket listings: fallo qcut/cut: {e}", "warning")

        if 'price_num' in self.calendar.columns:
            s = pd.to_numeric(self.calendar['price_num'], errors='coerce')
            try:
                if price_mode == 'quantile':
                    self.calendar['daily_price_bucket'] = pd.qcut(s, q=5, labels=price_labels or default_labels, duplicates='drop')
                elif price_mode == 'fixed' and price_bins is not None:
                    self.calendar['daily_price_bucket'] = pd.cut(s, bins=price_bins, labels=price_labels, include_lowest=True)
            except Exception as e:
                self.logs.log(f"[derive_features] daily_price_bucket calendar: fallo qcut/cut: {e}", "warning")

        self.logs.log(f"[derive_features] Fin | {self._shape_str()}", "info")
        return self

    # ---------------------------------------------------------------------
    # 4) Expandir campos anidados (amenities/verifications)
    # ---------------------------------------------------------------------
    def expand_nested_fields(self):
        """
        Propósito:
          - Convertir 'amenities' y 'host_verifications' a columnas dummies,
            usando un conjunto curado (evita explosión de columnas).

        Transformaciones:
          - Forzar listas válidas.
          - Normalizar a 'slugs' y crear dummies 'amenities_*' y 'host_verifications_*'.
          - Eliminar columnas originales anidadas.

        Logs:
          - Cantidad de columnas nuevas creadas por cada grupo.

        Retorna:
          - self
        """
        self.logs.log("[expand_nested_fields] Inicio", "info")

        amenities_keep = [
            "Wifi", "Dedicated workspace",
            "Self check-in", "Free parking on premises",
            "Kitchen", "Refrigerator", "Microwave", "Stove", "Coffee maker",
            "Cooking basics", "Dishes and silverware",
            "Room-darkening shades", "Essentials",
            "Washer",
            "Smoke alarm", "Carbon monoxide alarm", "Fire extinguisher", "First aid kit"
        ]
        verifications_keep = ["phone", "email", "work_email"]

        import ast
        df = self.listings

        def safe_slug(x: str) -> str:
            x = unicodedata.normalize('NFKD', str(x)).encode('ascii', 'ignore').decode('ascii')
            return re.sub(r'[^0-9a-zA-Z]+', '_', x).strip('_').lower()

        am_keep = set(safe_slug(a) for a in (amenities_keep or []))
        vr_keep = set(safe_slug(v) for v in (verifications_keep or []))

        # Asegurar listas
        for col in ['amenities', 'host_verifications']:
            if col in df.columns:
                def to_list(x):
                    if isinstance(x, list): return x
                    if isinstance(x, str) and x.strip().startswith('['):
                        try: return ast.literal_eval(x)
                        except: return []
                    if pd.isna(x) or x == '': return []
                    return [str(x)]
                df[col] = df[col].apply(to_list)

        # Dummies de amenities (subset)
        new_am_cols = []
        if 'amenities' in df.columns and am_keep:
            am_col_slugs = df['amenities'].apply(lambda lst: set(safe_slug(x) for x in lst if isinstance(x, str)))
            for a_slug in am_keep:
                col_name = f"amenities_{a_slug}"
                df[col_name] = am_col_slugs.apply(lambda s: int(a_slug in s))
                new_am_cols.append(col_name)
            df.drop(columns=['amenities'], inplace=True, errors='ignore')

        # Dummies de verifications (subset)
        new_vr_cols = []
        if 'host_verifications' in df.columns and vr_keep:
            vr_col_slugs = df['host_verifications'].apply(lambda lst: set(safe_slug(x) for x in lst if isinstance(x, str)))
            for v_slug in vr_keep:
                col_name = f"host_verifications_{v_slug}"
                df[col_name] = vr_col_slugs.apply(lambda s: int(v_slug in s))
                new_vr_cols.append(col_name)
            df.drop(columns=['host_verifications'], inplace=True, errors='ignore')

        self.listings = df
        self.logs.log(
            f"[expand_nested_fields] Fin | amenities={len(new_am_cols)} verifications={len(new_vr_cols)}",
            "info"
        )
        return self

    # ----------------- Fechas de review e imputación de precio -----------------

    def _fill_review_dates(self):
        """
        Propósito:
          - Completar first_review/last_review de listings con:
            1) min/max en reviews, 2) rango en calendar, 3) last_scraped,
            4) fallback '1970-01-01'.

        Logs:
          - Conteo por fuente (reviews/calendar/last_scraped/fallback) para first/last.

        Retorna:
          - self
        """
        self.logs.log("[_fill_review_dates] Inicio", "info")

        lst = self.listings
        cal = self.calendar
        rev = self.reviews

        # Fechas desde reviews
        if 'listing_id' in rev.columns and 'date' in rev.columns:
            rdt = pd.to_datetime(rev['date'], errors='coerce', utc=True)
            grp = (rev.assign(_dt=rdt).groupby('listing_id')['_dt'].agg(['min','max']).reset_index())
            grp['first_review_from_rev'] = grp['min'].dt.strftime('%Y-%m-%d')
            grp['last_review_from_rev']  = grp['max'].dt.strftime('%Y-%m-%d')
            m1_first = grp.set_index('listing_id')['first_review_from_rev']
            m1_last  = grp.set_index('listing_id')['last_review_from_rev']
        else:
            m1_first = pd.Series(dtype=object)
            m1_last  = pd.Series(dtype=object)

        # Rango desde calendar
        if 'listing_id' in cal.columns and 'date' in cal.columns:
            cdt = pd.to_datetime(cal['date'], errors='coerce', utc=True)
            cgrp = (cal.assign(_dt=cdt).groupby('listing_id')['_dt'].agg(['min','max']).reset_index())
            cgrp['first_review_from_cal'] = cgrp['min'].dt.strftime('%Y-%m-%d')
            cgrp['last_review_from_cal']  = cgrp['max'].dt.strftime('%Y-%m-%d')
            m2_first = cgrp.set_index('listing_id')['first_review_from_cal']
            m2_last  = cgrp.set_index('listing_id')['last_review_from_cal']
        else:
            m2_first = pd.Series(dtype=object)
            m2_last  = pd.Series(dtype=object)

        # Asegurar columnas
        if 'first_review' not in lst.columns: lst['first_review'] = ''
        if 'last_review'  not in lst.columns: lst['last_review']  = ''

        # Relleno con conteo de fuentes
        src_counts = {"reviews_first":0, "calendar_first":0, "last_scraped_first":0, "fallback_first":0,
                      "reviews_last":0,  "calendar_last":0,  "last_scraped_last":0,  "fallback_last":0}

        def _fill_by_map(colname, mapper1, mapper2, src_prefix):
            id_map1 = lst['id'].map(mapper1) if not mapper1.empty else None
            id_map2 = lst['id'].map(mapper2) if not mapper2.empty else None
            mask = (lst[colname].isna()) | (lst[colname].astype(str).str.strip() == '')
            # 1) reviews
            if id_map1 is not None and mask.any():
                to_fill = mask & id_map1.notna()
                src_counts[f"reviews_{src_prefix}"] += int(to_fill.sum())
                lst.loc[to_fill, colname] = lst.loc[to_fill, colname].fillna(id_map1[to_fill])
            # 2) calendar
            mask = (lst[colname].isna()) | (lst[colname].astype(str).str.strip() == '')
            if id_map2 is not None and mask.any():
                to_fill = mask & id_map2.notna()
                src_counts[f"calendar_{src_prefix}"] += int(to_fill.sum())
                lst.loc[to_fill, colname] = lst.loc[to_fill, colname].fillna(id_map2[to_fill])
            # 3) last_scraped
            mask = (lst[colname].isna()) | (lst[colname].astype(str).str.strip() == '')
            if 'last_scraped' in lst.columns and mask.any():
                src_counts[f"last_scraped_{src_prefix}"] += int(mask.sum())
                lst.loc[mask, colname] = lst.loc[mask, 'last_scraped']
            # 4) fallback fijo
            mask = (lst[colname].isna()) | (lst[colname].astype(str).str.strip() == '')
            src_counts[f"fallback_{src_prefix}"] += int(mask.sum())
            lst.loc[mask, colname] = '1970-01-01'

        _fill_by_map('first_review', m1_first, m2_first, 'first')
        _fill_by_map('last_review',  m1_last,  m2_last,  'last')

        self.listings = lst
        self.logs.log(f"[_fill_review_dates] Fin | fuentes: {src_counts}", "info")
        return self

    def _impute_listing_prices_and_buckets(self):
        """
        Propósito:
          - Imputar 'listing_price_num' con:
            1) mediana en calendar por listing,
            2) mediana por barrio,
            3) mediana global.
          - Garantizar y recalcular 'price_bucket'.

        Logs:
          - Cantidad imputada por fuente y modo de bucket usado.

        Retorna:
          - self
        """
        self.logs.log("[_impute_listing_prices_and_buckets] Inicio", "info")

        lst = self.listings
        cal = self.calendar

        # Asegurar columna base
        if 'price_num' in lst.columns and 'listing_price_num' not in lst.columns:
            lst['listing_price_num'] = pd.to_numeric(lst['price_num'], errors='coerce')
        if 'listing_price_num' not in lst.columns:
            lst['listing_price_num'] = np.nan

        filled_cal = filled_nb = 0

        # 1) Mediana calendar por listing
        if {'listing_id','price_num'}.issubset(cal.columns):
            med_cal = (cal.groupby('listing_id')['price_num'].median(numeric_only=True))
            mask = lst['listing_price_num'].isna()
            before = mask.sum()
            lst['listing_price_num'] = lst['listing_price_num'].fillna(lst['id'].map(med_cal))
            filled_cal = before - lst['listing_price_num'].isna().sum()

        # 2) Mediana por barrio
        if 'neighbourhood_cleansed' in lst.columns:
            mask = lst['listing_price_num'].isna()
            nb_med = (lst.groupby('neighbourhood_cleansed')['listing_price_num'].median(numeric_only=True))
            before = mask.sum()
            lst.loc[mask, 'listing_price_num'] = lst.loc[mask, 'neighbourhood_cleansed'].map(nb_med)
            filled_nb = before - lst['listing_price_num'].isna().sum()

        # 3) Mediana global
        mask = lst['listing_price_num'].isna()
        glob_med = pd.to_numeric(lst['listing_price_num'], errors='coerce').median()
        filled_glob = mask.sum()
        lst['listing_price_num'] = lst['listing_price_num'].fillna(glob_med)

        # Buckets (qcut o fallback por cuantiles)
        s = pd.to_numeric(lst['listing_price_num'], errors='coerce')
        default_labels = ['Very Low','Low','Medium','High','Very High']
        bucket_mode = "qcut"
        try:
            lst['price_bucket'] = pd.qcut(s, q=5, labels=default_labels, duplicates='drop')
        except Exception:
            bucket_mode = "cut-quantiles"
            qs = s.quantile([0, .2, .4, .6, .8, 1.0]).values
            bins = np.unique(qs)
            if len(bins) < 3:
                lst['price_bucket'] = 'Medium'
            else:
                labels = default_labels[:len(bins)-1]
                lst['price_bucket'] = pd.cut(s, bins=bins, labels=labels, include_lowest=True)

        # NaN residuales → 'Medium'
        if lst['price_bucket'].isna().any():
            n_na = lst['price_bucket'].isna().sum()
            lst['price_bucket'] = lst['price_bucket'].astype(object).fillna('Medium')
            self.logs.log(f"[_impute_listing_prices_and_buckets] price_bucket NaN→'Medium' ({n_na})", "warning")

        self.listings = lst
        self.logs.log(
            f"[_impute_listing_prices_and_buckets] Fin | from_calendar={filled_cal} "
            f"| by_neighbourhood={filled_nb} | global={filled_glob} | bucket_mode={bucket_mode}",
            "info"
        )
        return self

    # ---------------------------------------------------------------------
    # 5) Construcción de la sábana
    # ---------------------------------------------------------------------
    def build_flat_sheet(self):
        """
        Propósito:
          - Unir calendar con atributos de listing imputados y agregar reviews/mes.
          - Derivar booked_night y daily_revenue.

        Transformaciones:
          - booked_night = ~available; daily_revenue = booked_night * price_num.
          - Dimensión de listing: renombrar id → listing_id y price_num → listing_price_num.
          - Join calendar ← listings_dim por listing_id (alineando tipos).
          - reviews_in_month: conteo por (listing_id, year, month).
          - Garantías de no-nulo: reviews_in_month→0; first/last_review→'1970-01-01';
            listing_price_num→mediana; price_bucket→'Medium'.
          - Reordenar columnas clave al frente.

        Logs:
          - Inicio/fin, tamaño de resultado, columnas añadidas, normalizaciones.

        Retorna:
          - self
        """
        self.logs.log("[build_flat_sheet] Inicio", "info")

        cal = self.calendar.copy()
        lst = self.listings.copy()
        rev = self.reviews.copy()

        # Medidas diarias
        if 'available' in cal.columns:
            cal['booked_night'] = (~cal['available']).astype(int)
        else:
            cal['booked_night'] = np.nan
        if 'price_num' in cal.columns:
            cal['daily_revenue'] = cal['booked_night'] * cal['price_num']

        # Dummies ya expandidos
        amen_cols  = [c for c in lst.columns if c.startswith('amenities_')]
        verif_cols = [c for c in lst.columns if c.startswith('host_verifications_')]

        # Whitelist base
        base_keep = [
            'id','name','host_id','host_name','host_since','neighbourhood_cleansed','property_type','room_type',
            'accommodates','bedrooms','bathrooms','beds','instant_bookable',
            'latitude','longitude',
            'price_num','price_bucket',
            'review_scores_rating','review_scores_cleanliness','review_scores_accuracy','review_scores_communication',
            'review_scores_checkin','review_scores_location','review_scores_value','reviews_per_month',
            'host_is_superhost','host_identity_verified','host_has_profile_pic',
            'host_response_rate_pct','host_acceptance_rate_pct','host_response_time','host_total_listings_count',
            'availability_30','availability_60','availability_90','availability_365','number_of_reviews','number_of_reviews_ltm',
            'number_of_reviews_l30d','number_of_reviews_ly','first_review','last_review'
        ]
        keep = [c for c in (base_keep + amen_cols + verif_cols) if c in lst.columns]

        # Imputar precios/buckets antes del join
        self._impute_listing_prices_and_buckets()

        # Dimensión de listing para join
        lst = self.listings
        keep = [c for c in (base_keep + amen_cols + verif_cols) if c in lst.columns]
        lst_dim = lst[keep].rename(columns={'id':'listing_id', 'price_num':'listing_price_num'})

        # Join calendar ← listings_dim
        if 'listing_id' not in cal.columns:
            self.flat_sheet = cal.copy()
            self.logs.log("[build_flat_sheet] calendar SIN listing_id → se devuelve calendar sin join", "warning")
            return self
        lst_dim['listing_id'] = lst_dim['listing_id'].astype(str)
        cal['listing_id'] = cal['listing_id'].astype(str)
        before_join_rows = len(cal)
        flat = cal.merge(lst_dim, on='listing_id', how='left')
        self.logs.log(
            f"[build_flat_sheet] Join: filas calendar={before_join_rows} | "
            f"resultado={len(flat)} | col_listings_dim={len(lst_dim.columns)-1}",
            "info"
        )

        # Reviews por mes
        if 'date' in rev.columns:
            rdt = pd.to_datetime(rev['date'], errors='coerce', utc=True)
            rev['rev_year'], rev['rev_month'] = rdt.dt.year, rdt.dt.month
        if {'listing_id','rev_year','rev_month'}.issubset(rev.columns) and {'year','month'}.issubset(flat.columns):
            rev['listing_id'] = rev['listing_id'].astype(str)
            rmon = (rev.groupby(['listing_id','rev_year','rev_month']).size().reset_index(name='reviews_in_month'))
            flat = flat.merge(rmon, left_on=['listing_id','year','month'],
                              right_on=['listing_id','rev_year','rev_month'], how='left')
            flat.drop(columns=[c for c in ['rev_year','rev_month'] if c in flat.columns], inplace=True)
        else:
            flat['reviews_in_month'] = np.nan

        # Garantías de no-nulo
        if 'reviews_in_month' in flat.columns:
            n_na = flat['reviews_in_month'].isna().sum()
            flat['reviews_in_month'] = flat['reviews_in_month'].fillna(0).astype(int)
            if n_na:
                self.logs.log(f"[build_flat_sheet] reviews_in_month: {n_na} → 0", "info")

        for c in ['first_review','last_review']:
            if c in flat.columns:
                mask = (flat[c].isna()) | (flat[c].astype(str).str.strip() == '')
                n_fill = mask.sum()
                flat.loc[mask, c] = '1970-01-01'
                if n_fill:
                    self.logs.log(f"[build_flat_sheet] {c}: vacíos→'1970-01-01' ({n_fill})", "info")

        if 'listing_price_num' in flat.columns:
            med_lp = pd.to_numeric(flat['listing_price_num'], errors='coerce').median()
            n_na = flat['listing_price_num'].isna().sum()
            flat['listing_price_num'] = pd.to_numeric(flat['listing_price_num'], errors='coerce').fillna(med_lp)
            if n_na:
                self.logs.log(f"[build_flat_sheet] listing_price_num: {n_na} → mediana={med_lp}", "info")

        if 'price_bucket' in flat.columns:
            n_na = flat['price_bucket'].isna().sum()
            flat['price_bucket'] = flat['price_bucket'].astype(object).fillna('Medium')
            if n_na:
                self.logs.log(f"[build_flat_sheet] price_bucket: {n_na} → 'Medium'", "info")

        # Ordenar columnas clave primero
        front = ['listing_id','date','year','month','day','quarter','weekday','is_weekend',
                 'price_num','daily_price_bucket','available','booked_night','daily_revenue']
        front = [c for c in front if c in flat.columns]
        others = [c for c in flat.columns if c not in front]
        self.flat_sheet = flat[front + others]

        self.logs.log(f"[build_flat_sheet] Fin | flat_sheet={self.flat_sheet.shape}", "info")
        return self

    # ---------------------------------------------------------------------
    # 6) Pipeline completo
    # ---------------------------------------------------------------------
    def run(self, amenities_keep=None, verifications_keep=None,
            price_mode='quantile', price_bins=None, price_labels=None):
        """
        Propósito:
          - Ejecutar el flujo completo y devolver la sábana final.

        Logs:
          - Parámetros de ejecución y forma final de la sábana.

        Retorna:
          - pd.DataFrame (flat_sheet)
        """
        self.logs.log(
            f"[run] Inicio | price_mode={price_mode}, "
            f"price_bins={'set' if price_bins is not None else None}, "
            f"price_labels={'set' if price_labels is not None else None}",
            "info"
        )

        (self.normalize_types()
             .clean_nulls()
             .derive_features(price_mode=price_mode, price_bins=price_bins, price_labels=price_labels)
             .expand_nested_fields()
             .build_flat_sheet())

        self.logs.log(f"[run] Fin | flat_sheet={self.flat_sheet.shape}", "info")
        return self.flat_sheet
