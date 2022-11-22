# mekari_salary_per_hour

## Asumsi
Timesheet akan diasumsikan sebagai 8 jam working hour apabila:
1. data checkout atau checkin tidak ada (bernilai '')
2. waktu checkout lebihg kecil daripada waktu checkin

Terdapat employee yang memiliki 2 row records dengan nilai salary yang berbeda, yang diambil adalah data dengan nilai salary tertinggi.

Perhitungan pengeluaran branch didasari pada table timesheet. Namun table salary_period dan query sum_salary_per_period.sql dibuat sebagai alternatif.

## SQL
file query.sql merupakan file utama untuk pemecahan permasalahan yang ada, adapun query lainnya (selain sum_salary_per_period.sql) merupakan ddl pembentukan table pendukung untuk query tersebut. 
Seperti yang dispesifikasikan, solusi SQL ini menghitung salary per hour secara full-load.