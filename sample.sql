SELECT `t0`.`Akquisekanal__Level_1_` AS `Akquisekanal__Level_1_`,
  `t0`.`sum_Calculation_860187579479334912_ok` AS `sum_Calculation_860187579479334912_ok`,
  `t8`.`X_measure__3` AS `sum_Calculation_860187579479445505_ok`,
  `t5`.`X_measure__7` AS `usr_Calculation_491736835276996608_ok`,
  `t5`.`X_measure__11` AS `usr_Calculation_860187579489701894_ok`
FROM (
  SELECT `bestellartikel_tableau_view`.`Akquisekanal (Level 1)` AS `Akquisekanal__Level_1_`,
    SUM({fn CONVERT((CASE WHEN ((`bestellartikel_tableau_view`.`Flag Stornos` = 0) AND (`bestellartikel_tableau_view`.`Hauptkategorie` <> 'Virtuelle Artikel') AND (`bestellartikel_tableau_view`.`Hauptkategorie` <> 'Arbeitswerte') AND (`bestellartikel_tableau_view`.`Hauptkategorie` <> 'Unbekannt') AND (`bestellartikel_tableau_view`.`Hauptkategorie` <> 'N/A') AND (CASE WHEN 1 >= 0 THEN {fn LEFT(`bestellartikel_tableau_view`.`Rechnungsnummer`,1)} ELSE NULL END <> '-')) THEN 1 ELSE 0 END), SQL_BIGINT)}) AS `sum_Calculation_860187579479334912_ok`
  FROM `bestellartikel_tableau_view`
  WHERE ((`bestellartikel_tableau_view`.`Bestelldatum` >= {d '2014-01-01'}) AND (`bestellartikel_tableau_view`.`Bestelldatum` <= {d '2018-10-17'}))
  GROUP BY `Akquisekanal__Level_1_`
) `t0`
  INNER JOIN (
  SELECT `t1`.`Akquisekanal__Level_1_` AS `Akquisekanal__Level_1_`,
    AVG(CAST(`t2`.`X_measure__10` AS FLOAT)) AS `X_measure__11`,
    AVG(CAST(`t4`.`X_measure__6` AS FLOAT)) AS `X_measure__7`
  FROM (
    SELECT `bestellartikel_tableau_view`.`Akquisekanal (Level 1)` AS `Akquisekanal__Level_1_`,
      `bestellartikel_tableau_view`.`Auftragsnummer` AS `Auftragsnummer`
    FROM `bestellartikel_tableau_view`
    WHERE ((`bestellartikel_tableau_view`.`Bestelldatum` >= {d '2014-01-01'}) AND (`bestellartikel_tableau_view`.`Bestelldatum` <= {d '2018-10-17'}))
    GROUP BY `Akquisekanal__Level_1_`,
      `Auftragsnummer`
  ) `t1`
    INNER JOIN (
    SELECT `bestellartikel_tableau_view`.`Auftragsnummer` AS `Auftragsnummer`,
      SUM({fn CONVERT((CASE WHEN ((`bestellartikel_tableau_view`.`Flag Stornos` = 0) AND (`bestellartikel_tableau_view`.`Hauptkategorie` <> 'Virtuelle Artikel') AND (`bestellartikel_tableau_view`.`Hauptkategorie` <> 'Arbeitswerte') AND (`bestellartikel_tableau_view`.`Hauptkategorie` <> 'Unbekannt') AND (`bestellartikel_tableau_view`.`Hauptkategorie` <> 'N/A') AND (CASE WHEN 1 >= 0 THEN {fn LEFT(`bestellartikel_tableau_view`.`Rechnungsnummer`,1)} ELSE NULL END <> '-')) THEN 1 ELSE 0 END), SQL_BIGINT)}) AS `X_measure__10`
    FROM `bestellartikel_tableau_view`
    GROUP BY `Auftragsnummer`
  ) `t2` ON ((`t1`.`Auftragsnummer` = `t2`.`Auftragsnummer`) OR ((`t1`.`Auftragsnummer` IS NULL) AND (`t2`.`Auftragsnummer` IS NULL)))
    INNER JOIN (
    SELECT `t3`.`Auftragsnummer` AS `Auftragsnummer`,
      SUM({fn CONVERT((CASE WHEN (`t3`.`X_measure__2` > 0) THEN 1 ELSE 0 END), SQL_BIGINT)}) AS `X_measure__6`
    FROM (
      SELECT `bestellartikel_tableau_view`.`Artikelnummer` AS `Artikelnummer`,
        `bestellartikel_tableau_view`.`Auftragsnummer` AS `Auftragsnummer`,
        SUM((CASE WHEN ((`bestellartikel_tableau_view`.`Flag Stornos` = 0) AND (`bestellartikel_tableau_view`.`Hauptkategorie` <> 'Virtuelle Artikel') AND (`bestellartikel_tableau_view`.`Hauptkategorie` <> 'Arbeitswerte') AND (`bestellartikel_tableau_view`.`Hauptkategorie` <> 'Unbekannt') AND (`bestellartikel_tableau_view`.`Hauptkategorie` <> 'N/A')) THEN `bestellartikel_tableau_view`.`Anzahl Nettoauftragsartikel` ELSE 0 END)) AS `X_measure__2`
      FROM `bestellartikel_tableau_view`
      GROUP BY `Artikelnummer`,
        `Auftragsnummer`
    ) `t3`
    GROUP BY `Auftragsnummer`
  ) `t4` ON ((`t1`.`Auftragsnummer` = `t4`.`Auftragsnummer`) OR ((`t1`.`Auftragsnummer` IS NULL) AND (`t4`.`Auftragsnummer` IS NULL)))
  GROUP BY `Akquisekanal__Level_1_`
) `t5` ON ((`t0`.`Akquisekanal__Level_1_` = `t5`.`Akquisekanal__Level_1_`) OR ((`t0`.`Akquisekanal__Level_1_` IS NULL) AND (`t5`.`Akquisekanal__Level_1_` IS NULL)))
  INNER JOIN (
  SELECT `t6`.`Akquisekanal__Level_1_` AS `Akquisekanal__Level_1_`,
    SUM({fn CONVERT((CASE WHEN (`t7`.`X_measure__2` > 0) THEN 1 ELSE 0 END), SQL_BIGINT)}) AS `X_measure__3`
  FROM (
    SELECT `bestellartikel_tableau_view`.`Akquisekanal (Level 1)` AS `Akquisekanal__Level_1_`,
      `bestellartikel_tableau_view`.`Artikelnummer` AS `Artikelnummer`,
      `bestellartikel_tableau_view`.`Auftragsnummer` AS `Auftragsnummer`
    FROM `bestellartikel_tableau_view`
    WHERE ((`bestellartikel_tableau_view`.`Bestelldatum` >= {d '2014-01-01'}) AND (`bestellartikel_tableau_view`.`Bestelldatum` <= {d '2018-10-17'}))
    GROUP BY `Akquisekanal__Level_1_`,
      `Artikelnummer`,
      `Auftragsnummer`
  ) `t6`
    INNER JOIN (
    SELECT `bestellartikel_tableau_view`.`Artikelnummer` AS `Artikelnummer`,
      `bestellartikel_tableau_view`.`Auftragsnummer` AS `Auftragsnummer`,
      SUM((CASE WHEN ((`bestellartikel_tableau_view`.`Flag Stornos` = 0) AND (`bestellartikel_tableau_view`.`Hauptkategorie` <> 'Virtuelle Artikel') AND (`bestellartikel_tableau_view`.`Hauptkategorie` <> 'Arbeitswerte') AND (`bestellartikel_tableau_view`.`Hauptkategorie` <> 'Unbekannt') AND (`bestellartikel_tableau_view`.`Hauptkategorie` <> 'N/A')) THEN `bestellartikel_tableau_view`.`Anzahl Nettoauftragsartikel` ELSE 0 END)) AS `X_measure__2`
    FROM `bestellartikel_tableau_view`
    GROUP BY `Artikelnummer`,
      `Auftragsnummer`
  ) `t7` ON (((`t6`.`Artikelnummer` = `t7`.`Artikelnummer`) OR ((`t6`.`Artikelnummer` IS NULL) AND (`t7`.`Artikelnummer` IS NULL))) AND ((`t6`.`Auftragsnummer` = `t7`.`Auftragsnummer`) OR ((`t6`.`Auftragsnummer` IS NULL) AND (`t7`.`Auftragsnummer` IS NULL))))
  GROUP BY `Akquisekanal__Level_1_`
) `t8` ON ((`t0`.`Akquisekanal__Level_1_` = `t8`.`Akquisekanal__Level_1_`) OR ((`t0`.`Akquisekanal__Level_1_` IS NULL) AND (`t8`.`Akquisekanal__Level_1_` IS NULL))) FORMAT TabSeparatedWithNamesAndTypes;
