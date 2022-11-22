# mekari_salary_per_hour

## Asumsi
Timesheet akan diasumsikan sebagai 8 jam working hour apabila:
1. data checkout atau checkin tidak ada (bernilai '')
2. waktu checkout lebihg kecil daripada waktu checkin

Terdapat employee yang memiliki 2 row records dengan nilai salary yang berbeda, yang diambil adalah data dengan nilai salary tertinggi.

Perhitungan pengeluaran branch didasari pada table timesheet, sebagai contoh pada 2019-08 hanya branch_id 1, 2, dan 3 saja yang dihitung karena hanya ada 3 branct itu saja pada data timesheets.
Namun table salary_period dan query sum_salary_per_period.sql dibuat sebagai alternatif.

## SQL
file query.sql merupakan file utama untuk pemecahan permasalahan yang ada, adapun query lainnya (selain sum_salary_per_period.sql) merupakan ddl pembentukan table pendukung untuk query tersebut. 
Seperti yang dispesifikasikan, solusi SQL ini menghitung salary per hour secara full-load.

## Python (incremental)
Terdapat dua solusi python yang dibuat yaitu incremental menggunakan max date pada existing timesheet dan employee, serta incremental menggunakan periode berjalan.

###### Incremental Max Existing Date
Solusi ini menggunakan staging table yaitu untuk menyimpan data employee dan data timesheet. Inputan tetap diambil dari file csv namun dengan menggunakan filter max date proses ini hanya akan mengambil data data delta saja.

###### Incremental Periode Berjalan
Solusi ini mengasumsikan tidak akan ada perubahan pada periode sebelumnya serta data yang diproses dan sudah masuk table sudah lengkap dan up to date. Hal ini dikarenakan solusi ini hanya akan memproses periode berjalan saat ini saja, misalnya sekarang tanggal 2022-11-23, maka periode yang akan diproses hanya periode 2022-11. Jika memang akan ada perubahan pada periode sebelumnya sudah disediakan variable yang dapat diubah secara manual untuk memproses periode yang diinginkan.