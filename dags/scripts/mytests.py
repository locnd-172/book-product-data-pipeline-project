from scripts.check_data_quality import check_for_nulls
from scripts.check_data_quality import check_for_min_max
from scripts.check_data_quality import check_for_valid_values
from scripts.check_data_quality import check_for_duplicates

test1={
	"testname": "Check for nulls",
	"test": check_for_nulls,
	"column": "category",
	"table": "DIMCATEGORY"
}

test2={
	"testname":"Check for min and max",
	"test": check_for_min_max,
	"column": "rating_average",
	"table": "DIMREVIEW",
	"minimum": 0,
	"maximum": 5
}

test3={
	"testname":"Check for min and max",
	"test": check_for_min_max,
	"column": "percent_5_star",
	"table": "DIMREVIEW",
	"minimum": 0,
	"maximum": 1
}

test4={
	"testname":"Check for valid values",
	"test": check_for_valid_values,
	"column": "book_cover",
	"table": "DIMBOOK",
	"valid_values": {'Bìa mềm','Bìa cứng', 'Bìa gập', 'Bìa rời', 'Bìa da', 'Boxset', 'Paperback', 'Box'}
}

test5={
	"testname":"Check for duplicates",
	"test": check_for_duplicates,
	"column": "product_id",
	"table": "DIMBOOK"
}