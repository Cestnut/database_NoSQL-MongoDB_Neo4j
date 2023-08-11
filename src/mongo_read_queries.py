from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
mydb = client["calls_network"]

begin = 0
end = 2000000000000000
city = "Imperia"

people_collection = mydb["people"]
cells_collection = mydb["cells"]
calls_collection = mydb["calls"]

calls_collection.aggregate

"""
db.calls.find({"BEGIN_TIMESTAMP":{$gte: 0, $lte: {{}00}}})

db.calls.aggregate({
    $lookup:{
        from:"people", 
        localField:"CALLER",
        foreignField:"NUMBER",
        as: "caller"}
    },
    {
    $match:{
        "BEGIN_TIMESTAMP":{
            $gte:0, 
            $lte:{{}00}}
        }
    })

db.calls.aggregate(
    {
    $lookup:{
        from:"people", 
        localField:"CALLER",
        foreignField:"NUMBER",
        as: "caller"}
    },
    {
    $match:{
        "BEGIN_TIMESTAMP":{$gte:0, $lte:{{}00}}
        }
    },
    {
    $lookup:{
        from:"cells", 
        localField:"CELL_ID", 
        foreignField:"ID", as:"cell"}
    }
)


db.calls.aggregate({
    $lookup:{
        from:"people", 
        localField:"CALLER",
        foreignField:"NUMBER",
        as: "caller"}
    },
    {
    $match:{
        "BEGIN_TIMESTAMP":{$gte:0, $lte:{{}00}}
        }
    },
    {
    $lookup:{
        from:"cells", 
        localField:"CELL_ID", 
        foreignField:"ID", 
        as:"cell", 
        pipeline:[
                {
                    $match:{"CITY":{$eq:"Imperia"}}
                }
                ]
        }
    }, 
    {
    $match:{"cell":{$ne:[]}}
    }
)

db.calls.aggregate(
    {
        $lookup:{
            from:"people",
            localField:"CALLER",
            foreignField:"NUMBER",
            as: "caller"}
    },
    {
        $match:{
            "BEGIN_TIMESTAMP":{$gte:0, $lte:{{}00}}
        }
    },
    {
        $lookup:{
            from:"cells",
            localField:"CELL_ID",
            foreignField:"ID",
            as:"cell",
            pipeline:[
                {
                    $match:{
                        "CITY":{$eq:"Imperia"}}}
                ]}},
    {
        $match:{"cell":{$ne:[]}}
    },
    {
        $lookup:{
            from:"people",
            localField:"CALLED",
            foreignField:"NUMBER",
            as:"called"
            }
    }
)

"""

read_queries = [
    [
        {
            "$match":{
                "BEGIN_TIMESTAMP":{"$gte": {}, "$lte": {}}
            }
        }
    ],
    [
        {
            "$lookup":{
                "from":"people",
                "localField": "CALLER",
                "foreignField": "NUMBER",
                "as":"caller"
                }
        },
        {
            "$match":{
                "BEGIN_TIMESTAMP":{"$gte":begin, "$lte":end}
            }
        }
    ],
    [
       {
            "$lookup":{
                "from":"people",
                "localField": "CALLER",
                "foreignField": "NUMBER",
                "as":"caller"
                }
        },{
            "$match":{
                "BEGIN_TIMESTAMP":{"$gte":begin, "$lte":end}
            }
        },{
            "$lookup":{
                "from":"cells",
                "localField": "CELL_ID",
                "foreignField": "ID",
                "as":"cell"
                }
        }
    ],[
       {
            "$lookup":{
                "from":"people",
                "localField": "CALLER",
                "foreignField": "NUMBER",
                "as":"caller"
                }
        },{
            "$match":{
                "BEGIN_TIMESTAMP":{"$gte":begin, "$lte":end}
            }
        },{
            "$lookup":{
                "from":"cells",
                "localField": "CELL_ID",
                "foreignField": "ID",
                "as":"cell",
                "pipeline":[
                    {
                        "$match":{"CITY":{"$eq":city}}
                    }
                ]
                }
        },{
            "$match":{"cell":{"$ne":[]}}
        }
    ],[
       {
            "$lookup":{
                "from":"people",
                "localField": "CALLER",
                "foreignField": "NUMBER",
                "as":"caller"
                }
        },{
            "$match":{
                "BEGIN_TIMESTAMP":{"$gte":begin, "$lte":end}
            }
        },{
            "$lookup":{
                "from":"cells",
                "localField": "CELL_ID",
                "foreignField": "ID",
                "as":"cell",
                "pipeline":[
                    {
                        "$match":{"CITY":{"$eq":city}}
                    }
                ]
                }
        },{
            "$match":{"cell":{"$ne":[]}}
        },{
            "$lookup":{
                "from":"people",
                "localField": "CALLED",
                "foreignField": "NUMBER",
                "as":"called"
                }
        }
    ]
]


for query in read_queries:
    result = calls_collection.aggregate(query)
    for record in result:
        print(record)
    print("\n\n\n")