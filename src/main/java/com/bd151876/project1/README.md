# Zestaw 3 – nyc-accidents

Pochodzenie danych to `https://opendata.cityofnewyork.us/data/`

Uwaga! Dane pobieramy z miejsca wskazanego w ramach Twojego kursu

## Dwa zbiory danych

### `datasource1` – informacje na wypadków drogowych (1)

Dane mają format `CSV`, pliki nie posiadają nagłówka.

Pola w pliku:

0. `crash_date` - data wypadku,
1. `crash_time` - czas,
2. `zip_code` - kod pocztowy,
3. `latitude` - szerokość geograficzna,
4. `longitude` - długość geograficzna,
5. `location` - lokalizacja,
6. `on_street_name` - główna nazwa ulicy,
7. `cross_street_name` - nazwa ulicy krzyżującej się z główną,
8. `off_street_name` - nazwa bocznej ulicy,
9. `numer_of_persons_injured` - całkowita liczba rannych,
10. `numer_of_persons_killed` - całkowita liczba zabitych,
11. `numer_of_pedestrians_injured` - liczba rannych pieszych,
12. `numer_of_pedestrians_killed` - liczba zabitych pieszych,
13. `numer_of_cyclist_injured` - liczba rannych rowerzystów,
14. `numer_of_cyclist_killed` - liczba zabitych rowerzystów,
15. `numer_of_motorist_injured` - liczba rannych kierowców,
16. `numer_of_motorist_killed` - liczba zabitych kierowców,
17. `contributing_factor_vehicle_1` - współczynnik przyczyniający się do wypadku dla pojazdu 1,
18. `contributing_factor_vehicle_2` - współczynnik przyczyniający się do wypadku dla pojazdu 2,
19. `contributing_factor_vehicle_3` - współczynnik przyczyniający się do wypadku dla pojazdu 3,
20. `contributing_factor_vehicle_4` - współczynnik przyczyniający się do wypadku dla pojazdu 4,
21. `contributing_factor_vehicle_5` - współczynnik przyczyniający się do wypadku dla pojazdu 5,
22. `collision_id` - identyfikator wypadku,
23. `vehicle_type_code1` - kod typu pojazdu 1,
24. `vehicle_type_code2` - kod typu pojazdu 2,
25. `vehicle_type_code3` - kod typu pojazdu 3,
26. `vehicle_type_code4` - kod typu pojazdu 4,
27. `vehicle_type_code5` - kod typu pojazdu 5

### `datasource4` – informacje stref kodów pocztowych (4)

Dane mają format `CSV`, każdy z plików ma wiersz nagłówka.

Pola w pliku

0. `ZIP CODE` – kod pocztowy
1. `BOROUGH` – nazwa dzielnicy

## Program MapReduce (2)

Działając na zbiorze `datasource1` `(1)` należy dla każdej ulicy w zakresie należącym do określonej strefy pocztowej (w przypadku kiedy dla tej samej ulicy istnieją przypadki opisane różnymi strefami pocztowymi, należy je policzyć oddzielnie) wyznaczyć liczbę poszkodowanych z podziałem na typ poszkodowanych (pieszych, rowerzystów i kierowców) oraz charakter obrażeń (ranni, zabici). Wyniki mają być ograniczone tylko do wypadków mających miejsce po 2012 roku i opisanych kodem pocztowym.

W wynikowym zbiorze `(3)` powinny znaleźć się atrybuty:

* ulica,
* nr strefy pocztowej,
* typ poszkodowanych,
* charakter obrażeń,
* liczba poszkodowanych

Uwaga: Każde zdarzenie może być opisane trzema różnymi ulicami. W zależności od tego ile ulic zostało określonych,
dane zdarzenie należy zaliczyć na konto każdej z nich.

## Program Hive (5)
Działając na wyniku zadania MapReduce `(3)` oraz zbiorze danych `datasource4` `(4)` należy dla każdego typu poszkodowanych wyznaczyć trzy ulice leżące na terenie Manhatanu, dla których suma poszkodowanych (zabici+ranni) jest największa.

Wynik `(6)` powinien zawierać następujące atrybuty:

* `street` – ulica
* `person_type` – typ poszkodowanych
* `killed` – liczba zabitych
* `injured` – liczba rannych

Cyfry w nawiasach odnoszą się do cyfr wykorzystanych na graficznej reprezentacji projektu – patrz opis projektu na stronie kursu. 