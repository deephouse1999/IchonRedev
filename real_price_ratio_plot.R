library(dplyr)
library(readr)
library(stringr)
library(purrr)
library(plotly)
library(lubridate)
library(tidyr)

real_price_dir <- "C:/app/construct/real_price"
reference_complexes <- c("한가람")
reference_area_range <- c(59.5, 60.2)
comparison_complexes <- c("한가람", "대우", "강촌", "우성", "코오롱")
comparison_complex_pattern <- "^(한가람|강촌)$|대우|우성|코오롱"
recent_match_months <- 24
asking_price_negotiation_rate <- 0.05
focus_bldg_nm <- c("장미맨션", "정우맨션")
area_spread_threshold <- 66
max_area_tolerance <- 1

roll_median <- function(x, window = 3) {
  vapply(seq_along(x), function(i) {
    idx <- max(1, i - floor(window / 2)):min(length(x), i + floor(window / 2))
    median(x[idx], na.rm = TRUE)
  }, numeric(1))
}

add_manual_list_txt <- "
mn_lotno,sub_lotno,bldg_nm,siar,lat_num,long_num,build_reg_yn
0302,0216,충신교회주차장,,37.521516,126.963478,n
0302,0086,빌라맨션,4105.8,37.521200,126.964302,y
0302,0028,타워맨션,2472.4,37.520915,126.964985,y
0302,0140,퀸스리버빌,,37.520554,126.965451,y
0302,0079,kp빌딩,,37.521411,126.965554,y
0421,0000,강변복지소공원,377.5,37.521093,126.965865,y
0418,0000,센트레빌,6949.1,37.521425, 126.966461,y
0419,0000,센트레빌,4091.8,37.521425, 126.966461,y
0420,0000,센트레빌,1739.6,37.521425, 126.966461,y
0302,0069,정우맨션,1498.5,37.520891, 126.967185,y
0302,0075,의사협회,,37.520676,126.966216,y
0302,0068,풍원상가,1534.9,37.521332,126.967376,y
0302,0064,장미맨션,5053.2,37.520221,126.967625,y
0302,0090,이촌동면옥,330.6,37.520562,126.967566,y
0302,0091,충신교회유치원,,37.520502,126.967803,y
0302,0065,부대찌게(은진연립),661.2,37.520706,126.967701,y
0302,0062,코스모스맨션,,37.520812,126.968029,y
0302,0063,한강동부,,37.520503, 126.968110,y
0302,0067,충신교회본당,,37.521170, 126.968017,y
0302,0061,한강제일맨션,1379.5,37.521088, 126.968420,y
0302,0165,마산아구찜,,37.520702,126.968418,y
0302,0082,세븐일레븐,258.8,37.521165,126.968849,y
0302,0060,스타벅스,,37.521097,126.969049,y
0302,0080,백림,,37.520963,126.968842,y
0302,0081,백림,,37.520963,126.968842,y
0302,0053,월드,,37.520789,126.968996,y
0302,0054,미주A,1056.2,37.520533,126.968804,y
0302,0058,미주A,1079.3,37.520533,126.968804,y
0435,0000,보람,,37.520217,126.968725,y
0302,0048,미주B,1227.8,37.520326,126.969529,y
0302,0049,미주B,1231.1,37.520326,126.969529,y
0302,0050,미주B,1227.8,37.520326,126.969529,y
0302,0052,LG프라자,,37.521121,126.969296,y
0302,0051,국민은행,,37.520910,126.969785,y
0302,0045,노들맨션,,37.520173,126.969931,y
0302,0088,쉐이드트리,,37.519971,126.969856,y
0302,0046,곤트란쉐리에,,37.519814,126.969903,y
0302,0162,가온면옥,,37.519878,126.969724,y
0302,0031,도로,8191.7,37.520640,126.968430,n
"

target_bldg_nm <- c(
  "미주A", "미주B", "한강동부", "보람", "장미맨션", "한강제일맨션",
  "센트레빌", "노들맨션", "월드", "퀸스리버빌", "코스모스맨션",
  "빌라맨션", "타워맨션", "정우맨션"
)

add_manual_list <- read_csv(
  I(add_manual_list_txt),
  col_types = cols(.default = col_character()),
  show_col_types = FALSE
) %>%
  mutate(
    mn_lotno = str_pad(str_trim(mn_lotno), 4, side = "left", pad = "0"),
    sub_lotno = str_pad(str_trim(sub_lotno), 4, side = "left", pad = "0")
  )

real_price_col_names <- c(
  "row_no", "sigungu", "jibun", "lot_main_raw", "lot_sub_raw",
  "complex_nm_raw", "exclusive_area_raw", "contract_ym", "contract_day",
  "deal_price_raw", "dong", "floor", "buyer", "seller", "built_year",
  "road_nm", "cancel_date", "deal_type", "broker_location", "registration_date"
)

real_price_files <- list.files(
  real_price_dir,
  pattern = "\\.csv$",
  full.names = TRUE
)

if (length(real_price_files) == 0) {
  real_price_files <- Sys.glob(file.path(real_price_dir, "*.csv"))
}

if (length(real_price_files) == 0) {
  real_price_files <- list.files(
    real_price_dir,
    pattern = "\\.csv$",
    full.names = TRUE,
    recursive = TRUE
  )
}

real_price_files <- real_price_files[
  !startsWith(basename(real_price_files), "~") &
    !startsWith(basename(real_price_files), ".")
]

if (length(real_price_files) == 0) {
  stop("No real-price CSV files found in: ", real_price_dir)
}

real_price_loaded <- map_dfr(real_price_files, function(path) {
  x <- read_csv(
    path,
    skip = 16,
    col_names = real_price_col_names,
    locale = locale(encoding = "CP949"),
    col_types = cols(.default = col_character()),
    show_col_types = FALSE
  )

  if (ncol(x) < length(real_price_col_names)) {
    stop("Unexpected real-price CSV format: ", basename(path))
  }

  names(x)[seq_along(real_price_col_names)] <- real_price_col_names
  x
})

missing_real_price_cols <- setdiff(real_price_col_names, names(real_price_loaded))
if (length(missing_real_price_cols) > 0) {
  stop("Missing real-price columns after import: ", paste(missing_real_price_cols, collapse = ", "))
}

real_price_raw <- real_price_loaded %>%
  filter(!is.na(.data[["complex_nm_raw"]]), !is.na(.data[["contract_ym"]])) %>%
  mutate(
    complex_nm = .data[["complex_nm_raw"]],
    lot_main = str_pad(str_trim(.data[["lot_main_raw"]]), 4, side = "left", pad = "0"),
    lot_sub = str_pad(str_trim(.data[["lot_sub_raw"]]), 4, side = "left", pad = "0"),
    exclusive_area = parse_number(.data[["exclusive_area_raw"]]),
    deal_month = as.Date(paste0(.data[["contract_ym"]], "01"), format = "%Y%m%d"),
    deal_price = parse_number(.data[["deal_price_raw"]]),
    price_per_m2 = deal_price / exclusive_area
  ) %>%
  filter(is.na(.data[["cancel_date"]]) | .data[["cancel_date"]] %in% c("", "-"))

target_lots <- add_manual_list %>%
  filter(bldg_nm %in% target_bldg_nm) %>%
  transmute(
    bldg_nm,
    lot_main = mn_lotno,
    lot_sub = sub_lotno
  ) %>%
  distinct()

reference_monthly <- real_price_raw %>%
  filter(
    complex_nm %in% reference_complexes,
    between(exclusive_area, reference_area_range[1], reference_area_range[2])
  ) %>%
  group_by(deal_month) %>%
  summarise(
    ref_price = median(deal_price, na.rm = TRUE),
    ref_price_per_m2 = median(price_per_m2, na.rm = TRUE),
    ref_n = n(),
    .groups = "drop"
  )

target_real_price <- real_price_raw %>%
  inner_join(target_lots, by = c("lot_main", "lot_sub")) %>%
  group_by(bldg_nm) %>%
  mutate(
    min_area_by_bldg = min(exclusive_area, na.rm = TRUE),
    max_area_by_bldg = max(exclusive_area, na.rm = TRUE),
    area_spread_by_bldg = max_area_by_bldg - min_area_by_bldg,
    use_max_area_only = area_spread_by_bldg >= area_spread_threshold,
    representative_area = if_else(use_max_area_only, max_area_by_bldg, exclusive_area)
  ) %>%
  ungroup() %>%
  filter(
    !use_max_area_only |
      exclusive_area >= representative_area - max_area_tolerance
  )

target_area_rule <- target_real_price %>%
  group_by(bldg_nm) %>%
  summarise(
    selected_min_area = min(exclusive_area, na.rm = TRUE),
    selected_max_area = max(exclusive_area, na.rm = TRUE),
    selected_n = n(),
    .groups = "drop"
  )

target_monthly <- target_real_price %>%
  group_by(bldg_nm, deal_month) %>%
  summarise(
    target_price = median(deal_price, na.rm = TRUE),
    target_price_per_m2 = median(price_per_m2, na.rm = TRUE),
    target_n = n(),
    .groups = "drop"
  )

ratio_ts <- target_monthly %>%
  inner_join(reference_monthly, by = "deal_month") %>%
  group_by(bldg_nm) %>%
  arrange(deal_month, .by_group = TRUE) %>%
  mutate(
    ratio = target_price / ref_price,
    ratio_per_m2 = target_price_per_m2 / ref_price_per_m2,
    gap_price = ref_price - target_price,
    premium_gap_price = target_price - ref_price,
    gap_price_per_m2 = ref_price_per_m2 - target_price_per_m2,
    ratio_roll = roll_median(ratio),
    ratio_per_m2_roll = roll_median(ratio_per_m2),
    gap_price_roll = roll_median(gap_price),
    premium_gap_price_roll = roll_median(premium_gap_price),
    gap_price_per_m2_roll = roll_median(gap_price_per_m2),
    hover_txt = paste0(
      bldg_nm,
      "<br>월: ", format(deal_month, "%Y-%m"),
      "<br>㎡당 기준 대비: ", round(ratio_per_m2 * 100, 1), "%",
      "<br>㎡당 기준 대비(rolling): ", round(ratio_per_m2_roll * 100, 1), "%",
      "<br>거래금액 기준 대비: ", round(ratio * 100, 1), "%",
      "<br>거래금액 기준 대비(rolling): ", round(ratio_roll * 100, 1), "%",
      "<br>대상 월중간값: ", format(round(target_price), big.mark = ","), "만원",
      "<br>기준 월중간값: ", format(round(ref_price), big.mark = ","), "만원",
      "<br>총액 프리미엄: ", format(round(premium_gap_price), big.mark = ","), "만원",
      "<br>㎡당 가격차: ", format(round(gap_price_per_m2), big.mark = ","), "만원/㎡",
      "<br>대상 거래수: ", target_n,
      "<br>기준 거래수: ", ref_n
    )
  ) %>%
  ungroup() %>%
  arrange(bldg_nm, deal_month)

ratio_plot <- plot_ly(
  ratio_ts,
  x = ~deal_month,
  y = ~ratio_per_m2_roll,
  color = ~bldg_nm,
  type = "scatter",
  mode = "lines+markers",
  text = ~hover_txt,
  hoverinfo = "text"
) %>%
  layout(
    title = "한가람 59㎡대 월별 중간값 대비 실거래가 비율",
    xaxis = list(title = "계약월"),
    yaxis = list(title = "한가람 59㎡ 대비 ㎡당가격 비율", tickformat = ".0%"),
    legend = list(title = list(text = "bldg_nm")),
    hovermode = "closest",
    shapes = list(
      list(
        type = "line",
        xref = "paper",
        x0 = 0,
        x1 = 1,
        yref = "y",
        y0 = 1,
        y1 = 1,
        line = list(color = "gray50", width = 1.5, dash = "dash")
      )
    ),
    annotations = list(
      list(
        xref = "paper",
        x = 1,
        y = 1,
        xanchor = "right",
        yanchor = "bottom",
        text = "100% 기준선: 한가람과 동일한 ㎡당 가격",
        showarrow = FALSE,
        font = list(size = 11, color = "gray35")
      )
    )
  )

focus_ratio_ts <- ratio_ts %>%
  filter(bldg_nm %in% focus_bldg_nm) %>%
  mutate(
    focus_ratio = ratio,
    focus_gap_price = premium_gap_price
  )

focus_ratio_y_range <- c(
  0.8,
  max(focus_ratio_ts$focus_ratio, na.rm = TRUE) * 1.08
)

early_annotation <- focus_ratio_ts %>%
  filter(deal_month <= as.Date("2013-12-31")) %>%
  arrange(desc(focus_ratio)) %>%
  slice(1)

late_annotation <- focus_ratio_ts %>%
  filter(deal_month >= as.Date("2020-01-01"), focus_ratio <= 1) %>%
  arrange(deal_month) %>%
  slice(1)

if (nrow(late_annotation) == 0) {
  late_annotation <- focus_ratio_ts %>%
    arrange(desc(deal_month)) %>%
    slice(1)
}

focus_ratio_plot <- plot_ly(
  focus_ratio_ts,
  x = ~deal_month,
  y = ~focus_ratio,
  color = ~bldg_nm,
  type = "scatter",
  mode = "lines+markers",
  text = ~hover_txt,
  hoverinfo = "text"
) %>%
  layout(
    title = "장미맨션·정우맨션: 한가람 59㎡ 대비 월별 중간 거래가격 비율",
    xaxis = list(title = "계약월"),
    yaxis = list(
      title = "한가람 59㎡ 대비 거래가격 비율",
      tickformat = ".0%",
      range = focus_ratio_y_range
    ),
    legend = list(title = list(text = "bldg_nm")),
    hovermode = "closest",
    shapes = list(
      list(
        type = "line",
        xref = "paper",
        x0 = 0,
        x1 = 1,
        yref = "y",
        y0 = 1,
        y1 = 1,
        line = list(color = "gray40", width = 1.6, dash = "dash")
      )
    ),
    annotations = list(
      list(
        x = early_annotation$deal_month[1],
        y = early_annotation$focus_ratio[1],
        text = "2010년대 초반:<br>한가람 대비 2배 안팎 프리미엄",
        showarrow = TRUE,
        arrowhead = 2,
        ax = 55,
        ay = -45,
        font = list(size = 12)
      ),
      list(
        x = late_annotation$deal_month[1],
        y = late_annotation$focus_ratio[1],
        text = "2020년 이후:<br>정우맨션 100% 미만 진입",
        showarrow = TRUE,
        arrowhead = 2,
        ax = -70,
        ay = -45,
        font = list(size = 12)
      ),
      list(
        xref = "paper",
        x = 1,
        y = 1,
        xanchor = "right",
        yanchor = "bottom",
        text = "100% = 한가람 59㎡와 동일한 거래가격",
        showarrow = FALSE,
        font = list(size = 11, color = "gray35")
      )
    )
  )

focus_gap_plot <- plot_ly(
  focus_ratio_ts,
  x = ~deal_month,
  y = ~focus_gap_price,
  color = ~bldg_nm,
  type = "scatter",
  mode = "lines+markers",
  text = ~hover_txt,
  hoverinfo = "text"
) %>%
  layout(
    title = "한가람 59㎡ 대비 총 거래가격 프리미엄 축소",
    xaxis = list(title = "계약월"),
    yaxis = list(title = "대상 단지 - 한가람 59㎡ 거래가격 차이(만원)"),
    legend = list(title = list(text = "bldg_nm")),
    hovermode = "closest",
    shapes = list(
      list(
        type = "line",
        xref = "paper",
        x0 = 0,
        x1 = 1,
        yref = "y",
        y0 = 0,
        y1 = 0,
        line = list(color = "gray50", width = 1.2, dash = "dash")
      )
    )
  )

price_change_table <- ratio_ts %>%
  filter(!bldg_nm %in% focus_bldg_nm) %>%
  group_by(bldg_nm) %>%
  arrange(deal_month, .by_group = TRUE) %>%
  summarise(
    first_month = first(deal_month),
    last_month = last(deal_month),
    first_price = first(target_price),
    last_price = last(target_price),
    first_ref_price = first(ref_price),
    last_ref_price = last(ref_price),
    first_ratio = first(ratio),
    last_ratio = last(ratio),
    ratio_change_p = (last_ratio - first_ratio) * 100,
    ratio_change_pct = (last_ratio / first_ratio - 1) * 100,
    first_n = first(target_n),
    last_n = last(target_n),
    .groups = "drop"
  ) %>%
  left_join(target_area_rule, by = "bldg_nm") %>%
  arrange(last_ratio) %>%
  mutate(
    first_month = format(first_month, "%Y-%m"),
    last_month = format(last_month, "%Y-%m"),
    selected_area = if_else(
      abs(selected_max_area - selected_min_area) < 0.1,
      sprintf("%.1f㎡", selected_max_area),
      sprintf("%.1f~%.1f㎡", selected_min_area, selected_max_area)
    ),
    first_price = format(round(first_price), big.mark = ","),
    last_price = format(round(last_price), big.mark = ","),
    first_ref_price = format(round(first_ref_price), big.mark = ","),
    last_ref_price = format(round(last_ref_price), big.mark = ","),
    first_ratio = paste0(round(first_ratio * 100, 1), "%"),
    last_ratio = paste0(round(last_ratio * 100, 1), "%"),
    ratio_change_p = paste0(round(ratio_change_p, 1), "%p"),
    ratio_change_pct = paste0(round(ratio_change_pct, 1), "%")
  ) %>%
  select(
    bldg_nm,
    selected_area,
    first_month,
    first_price,
    first_ref_price,
    first_ratio,
    first_n,
    last_month,
    last_price,
    last_ref_price,
    last_ratio,
    last_n,
    ratio_change_p,
    ratio_change_pct
  )

price_change_plot <- plot_ly(
  type = "table",
  header = list(
    values = c(
      "단지명", "적용 전용면적", "최초월", "최초 중간가",
      "최초 한가람", "최초 한가람 대비", "최초 거래수",
      "최종월", "최종 중간가", "최종 한가람", "최종 한가람 대비",
      "최종 거래수", "비율 변화", "비율 변화율"
    ),
    align = "center"
  ),
  cells = list(
    values = as.list(price_change_table),
    align = "center"
  )
)

valuation_month <- max(real_price_raw$deal_month, na.rm = TRUE)

jangmi_index_ts <- target_real_price %>%
  filter(bldg_nm == "장미맨션") %>%
  group_by(deal_month) %>%
  summarise(
    jangmi_price = median(deal_price, na.rm = TRUE),
    jangmi_n = n(),
    .groups = "drop"
  ) %>%
  arrange(deal_month) %>%
  mutate(month_num = as.numeric(deal_month))

if (nrow(jangmi_index_ts) < 4) {
  stop("장미맨션 가격 index를 만들 거래 관측치가 부족합니다.")
}

jangmi_spline <- smooth.spline(
  x = jangmi_index_ts$month_num,
  y = log(jangmi_index_ts$jangmi_price)
)

predict_jangmi_index <- function(month) {
  exp(predict(jangmi_spline, as.numeric(month))$y)
}

valuation_jangmi_index <- predict_jangmi_index(valuation_month)

asking_price_raw <- tibble::tribble(
  ~bldg_nm, ~asking_price_uk, ~asking_area_m2, ~asking_price_per_pyeong, ~asking_source_date, ~asking_source,
  "퀸스리버빌", 32.5, 229, 4692, "2026-05-01", "네이버부동산 지도",
  "타워맨션", 30.0, 170, 5834, "2026-05-01", "네이버부동산 지도",
  "빌라맨션", 32.5, 229, 4692, "2026-05-01", "네이버부동산 지도",
  "장미맨션", 36.0, 207, 5749, "2026-05-01", "네이버부동산 지도",
  "코스모스맨션", 23.0, 269, 2827, "2026-05-01", "네이버부동산 지도",
  "정우맨션", 23.0, 136, 4618, "2026-05-01", "네이버부동산 지도",
  "한강동부", 20.5, 102, 6644, "2026-05-01", "네이버부동산 지도",
  "한강제일맨션", 19.0, 136, 4618, "2026-05-01", "네이버부동산 지도",
  "미주A", 25.0, 213, 3880, "2026-05-01", "네이버부동산 지도",
  "보람", 21.5, 109, 6521, "2026-05-01", "네이버부동산 지도",
  "미주B", 20.0, 159, 4158, "2026-05-01", "네이버부동산 지도",
  "노들맨션", 28.0, 227, 4078, "2026-05-01", "네이버부동산 지도",
  "센트레빌", 31.0, 132, 7764, "2026-05-01", "네이버부동산 지도",
  "월드", 22.5, 128, 5811, "2026-05-01", "네이버부동산 지도"
) %>%
  mutate(asking_price = asking_price_uk * 10000)

asking_price_by_bldg <- asking_price_raw %>%
  group_by(bldg_nm) %>%
  summarise(
    asking_price = median(asking_price, na.rm = TRUE),
    asking_area_m2 = median(asking_area_m2, na.rm = TRUE),
    asking_price_per_pyeong = median(asking_price_per_pyeong, na.rm = TRUE),
    asking_source_date = paste(sort(unique(asking_source_date)), collapse = ", "),
    asking_source = paste(sort(unique(asking_source)), collapse = ", "),
    asking_n = n(),
    .groups = "drop"
  )

target_latest_trade <- target_real_price %>%
  group_by(bldg_nm) %>%
  filter(deal_month == max(deal_month, na.rm = TRUE)) %>%
  summarise(
    last_trade_month = max(deal_month, na.rm = TRUE),
    last_trade_area = median(exclusive_area, na.rm = TRUE),
    last_trade_price = median(deal_price, na.rm = TRUE),
    last_trade_n = n(),
    .groups = "drop"
  )

target_valuation <- asking_price_by_bldg %>%
  left_join(target_latest_trade, by = "bldg_nm") %>%
  mutate(
    target_value_area = asking_area_m2,
    target_value_n = asking_n,
    last_jangmi_index = if_else(
      !is.na(last_trade_month),
      predict_jangmi_index(last_trade_month),
      NA_real_
    ),
    estimated_price = if_else(
      !is.na(last_trade_price) & !is.na(last_jangmi_index),
      last_trade_price * valuation_jangmi_index / last_jangmi_index,
      NA_real_
    ),
    asking_negotiated_price = asking_price * (1 - asking_price_negotiation_rate),
    asking_gap_pct = asking_negotiated_price / estimated_price - 1,
    asking_source_month = as.Date(paste0(substr(asking_source_date, 1, 7), "-01")),
    target_value_price = asking_negotiated_price,
    target_value_month = asking_source_month,
    index_adjustment_pct = estimated_price / last_trade_price - 1,
    valuation_basis = "최근 호가 5% 네고 반영"
  )

comparison_recent_month <- real_price_raw %>%
  filter(str_detect(complex_nm, comparison_complex_pattern)) %>%
  summarise(recent_month = max(deal_month, na.rm = TRUE)) %>%
  pull(recent_month)

comparison_recent_price <- real_price_raw %>%
  filter(
    str_detect(complex_nm, comparison_complex_pattern),
    deal_month == comparison_recent_month
  ) %>%
  mutate(area_band = round(exclusive_area, 1)) %>%
  group_by(complex_nm, area_band) %>%
  summarise(
    comp_recent_month = max(deal_month, na.rm = TRUE),
    comp_recent_area = median(exclusive_area, na.rm = TRUE),
    comp_recent_price = median(deal_price, na.rm = TRUE),
    comp_recent_n = n(),
    .groups = "drop"
  ) %>%
  rename(comp_complex = complex_nm)

recent_price_match_candidates <- target_valuation %>%
  tidyr::crossing(comparison_recent_price) %>%
  mutate(
    target_price_per_m2 = target_value_price / target_value_area,
    comp_price_per_m2 = comp_recent_price / comp_recent_area,
    price_gap = target_value_price - comp_recent_price,
    abs_price_gap = abs(price_gap),
    price_gap_pct = price_gap / comp_recent_price,
    area_gap = target_value_area - comp_recent_area,
    abs_area_gap = abs(area_gap),
    pyeong_price_gap_pct = target_price_per_m2 / comp_price_per_m2 - 1
  ) %>%
  group_by(bldg_nm) %>%
  arrange(abs_price_gap, abs_area_gap, .by_group = TRUE) %>%
  mutate(match_rank = row_number()) %>%
  ungroup()

mapping_diagnostic_table <- recent_price_match_candidates %>%
  filter(match_rank <= 10) %>%
  transmute(
    bldg_nm,
    target_value_month = format(target_value_month, "%Y-%m"),
    target_value_area = sprintf("%.1f㎡", target_value_area),
    target_value_price = format(round(target_value_price), big.mark = ","),
    target_price_per_pyeong = format(round(target_price_per_m2 * 3.3058), big.mark = ","),
    comp_complex,
    comp_recent_area = sprintf("%.1f㎡", comp_recent_area),
    comp_recent_month = format(comp_recent_month, "%Y-%m"),
    comp_recent_price = format(round(comp_recent_price), big.mark = ","),
    comp_price_per_pyeong = format(round(comp_price_per_m2 * 3.3058), big.mark = ","),
    price_gap = format(round(price_gap), big.mark = ","),
    price_gap_pct = paste0(round(price_gap_pct * 100, 1), "%"),
    area_gap = paste0(round(area_gap, 1), "㎡"),
    pyeong_price_gap_pct = paste0(round(pyeong_price_gap_pct * 100, 1), "%"),
    match_rank
  )

comparison_price_history <- real_price_raw %>%
  filter(str_detect(complex_nm, comparison_complex_pattern)) %>%
  mutate(area_band = round(exclusive_area, 1)) %>%
  group_by(complex_nm, area_band, deal_month) %>%
  summarise(
    comp_month = max(deal_month, na.rm = TRUE),
    comp_area = median(exclusive_area, na.rm = TRUE),
    comp_price = median(deal_price, na.rm = TRUE),
    comp_n = n(),
    .groups = "drop"
  ) %>%
  rename(comp_complex = complex_nm)

recent_price_match_raw <- recent_price_match_candidates %>%
  group_by(bldg_nm) %>%
  arrange(abs_price_gap, abs(target_value_area - comp_recent_area), .by_group = TRUE) %>%
  slice_head(n = 1) %>%
  ungroup() %>%
  arrange(desc(target_value_price))

current_price_match_raw <- recent_price_match_raw %>%
  mutate(
    current_shortfall_price = pmax(comp_recent_price - target_value_price, 0)
  )

current_price_match_table <- current_price_match_raw %>%
  transmute(
    bldg_nm,
    target_value_month = format(target_value_month, "%Y-%m"),
    target_value_area = sprintf("%.1f㎡", target_value_area),
    target_value_price = format(round(target_value_price), big.mark = ","),
    valuation_basis,
    comp_complex,
    comp_recent_area = sprintf("%.1f㎡", comp_recent_area),
    comp_recent_month = format(comp_recent_month, "%Y-%m"),
    comp_recent_price = format(round(comp_recent_price), big.mark = ","),
    comp_recent_n,
    price_gap = format(round(price_gap), big.mark = ","),
    price_gap_pct = paste0(round(price_gap_pct * 100, 1), "%"),
    current_shortfall_price = if_else(
      current_shortfall_price > 0,
      format(round(current_shortfall_price), big.mark = ","),
      "-"
    )
  )

initial_current_match_raw <- current_price_match_raw
initial_current_match_table <- current_price_match_table

recent_price_match_table <- recent_price_match_raw %>%
  transmute(
    bldg_nm,
    last_trade_month = format(last_trade_month, "%Y-%m"),
    last_trade_price = format(round(last_trade_price), big.mark = ","),
    estimated_price = format(round(estimated_price), big.mark = ","),
    asking_price = if_else(
      is.na(asking_price),
      "-",
      format(round(asking_price), big.mark = ",")
    ),
    asking_negotiated_price = if_else(
      is.na(asking_negotiated_price),
      "-",
      format(round(asking_negotiated_price), big.mark = ",")
    ),
    asking_gap_pct = if_else(
      is.na(asking_gap_pct),
      "-",
      paste0(round(asking_gap_pct * 100, 1), "%")
    ),
    target_value_month = format(target_value_month, "%Y-%m"),
    target_value_area = sprintf("%.1f㎡", target_value_area),
    target_value_price = format(round(target_value_price), big.mark = ","),
    index_adjustment_pct = paste0(round(index_adjustment_pct * 100, 1), "%"),
    target_value_n,
    valuation_basis,
    comp_complex,
    comp_recent_area = sprintf("%.1f㎡", comp_recent_area),
    comp_recent_month = format(comp_recent_month, "%Y-%m"),
    comp_recent_price = format(round(comp_recent_price), big.mark = ","),
    comp_recent_n,
    price_gap = format(round(price_gap), big.mark = ","),
    price_gap_pct = paste0(round(price_gap_pct * 100, 1), "%")
  )

focus_ratio_plot
