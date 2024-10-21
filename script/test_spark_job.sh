spark-submit --deploy-mode client --class lake/test/.test_spark_crt_cif.py --date 2024-10-20
spark-submit --deploy-mode client --class lake/test/.test_spark_crt_product.py --date 2024-10-20
spark-submit --deploy-mode client --class lake/test/.test_spark_crt_dep_dtls.py --date 2024-10-20
spark-submit --deploy-mode client --class lake/test/.test_spark_ist_cif.py --date 2024-10-20
spark-submit --deploy-mode client --class lake/test/.test_spark_ist_product.py --date 2024-10-20
spark-submit --deploy-mode client --class lake/test/.test_spark_ist_dep_smy.py --date 2024-10-20
