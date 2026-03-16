let
    Source = period_ew,
    #"Filtered Rows" = Table.SelectRows(Source, each Text.Contains([Name], "aod_")),
    #"https://nada blob core windows net/atom-matching_20231001-20231231/errors_warnings/aod_20231001-20231231_warn csv" = #"Filtered Rows"{[#"Folder Path"=#"atom-matching-blob-url",Name=p_str &"/errors_warnings/aod_"&p_str&"_warn.csv"]}[Content],
    #"Imported CSV" = Csv.Document(#"https://nada blob core windows net/atom-matching_20231001-20231231/errors_warnings/aod_20231001-20231231_warn csv",[Delimiter=",", Columns=5, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Changed Type" = Table.TransformColumnTypes(#"Imported CSV",{{"Column1", type text}, {"Column2", type text}, {"Column3", type text}, {"Column4", type text}, {"Column5", type text}}),
    #"Promoted Headers" = Table.PromoteHeaders(#"Changed Type", [PromoteAllScalars=true]),
    #"Changed Type1" = Table.TransformColumnTypes(#"Promoted Headers",{{"SLK", type text}, {"RowKey", type text}, {"drug_name", type text}, {"field_name", type text}, {"field_value", type text}})
in
    #"Changed Type1"