Remove-Item E:\Data\Projects\CTA_V7\* -Recurse

$bgn_date_avlb = "20120104"
$stp_date = "20260201"

python main.py --bgn $bgn_date_avlb --stp $stp_date avlb
python main.py --bgn $bgn_date_avlb --stp $stp_date mkt
python main.py --bgn $bgn_date_avlb --stp $stp_date css
python main.py --bgn $bgn_date_avlb --stp $stp_date icov
