use dfs.tpchView;

create or replace  view region_csvScaleFactor as select
    cast(columns[0] as int) r_regionkey,
    cast(columns[1] as char(25)) r_name,
    cast(columns[2] as varchar(152)) r_comment
from dfs.`/tpchRaw/SFScaleFactor/region`;

create or replace  view nation_csvScaleFactor as select 
    cast(columns[0] as int) n_nationkey,
    cast(columns[1] as char(25)) n_name,
    cast(columns[2] as int) n_regionkey,
    cast(columns[3] as varchar(152)) n_comment
from dfs.`/tpchRaw/SFScaleFactor/nation`;

create or replace  view part_csvScaleFactor as select
    cast(columns[0] as bigint) p_partkey,
    cast(columns[1] as varchar(55)) p_name,
    cast(columns[2] as char(25)) p_mfgr,
    cast(columns[3] as char(10)) p_brand,
    cast(columns[4] as varchar(25)) p_type,
    cast(columns[5] as int) p_size,
    cast(columns[6] as char(10)) p_container,
    cast(columns[7] as double) p_retailprice,
    cast(columns[8] as varchar(23)) p_comment
from dfs.`/tpchRaw/SFScaleFactor/part`;


create or replace  view supplier_csvScaleFactor as select
    cast(columns[0] as bigint) s_suppkey,
    cast(columns[1] as char(25)) s_name,
    cast(columns[2] as varchar(40)) s_address,
    cast(columns[3] as int) s_nationkey,
    cast(columns[4] as char(15)) s_phone,
    cast(columns[5] as double) s_acctbal,
    cast(columns[6] as varchar(101)) s_comment
from dfs.`/tpchRaw/SFScaleFactor/supplier`;

create or replace  view partsupp_csvScaleFactor as select
    cast(columns[0] as bigint) ps_partkey,
    cast(columns[1] as bigint) ps_suppkey,
    cast(columns[2] as int) ps_availqty,
    cast(columns[3] as double) ps_supplycost,
    cast(columns[4] as varchar(199)) ps_comment
from dfs.`/tpchRaw/SFScaleFactor/partsupp`;


create or replace  view customer_csvScaleFactor as select
    cast(columns[0] as bigint) c_custkey,
    cast(columns[1] as char(25)) c_name,
    cast(columns[2] as varchar(40)) c_address,
    cast(columns[3] as int) c_nationkey,
    cast(columns[4] as char(15)) c_phone,
    cast(columns[5] as double) c_acctbal,
    cast(columns[6] as char(10)) c_mktsegment,
    cast(columns[7] as varchar(101)) c_comment
from dfs.`/tpchRaw/SFScaleFactor/customer`;


create or replace  view orders_csvScaleFactor as select
    cast(columns[0] as bigint) o_orderkey,
    cast(columns[1] as bigint) o_custkey,
    cast(columns[2] as char(1)) o_orderstatus,
    cast(columns[3] as double) o_totalprice,
    cast(columns[4] as date) o_orderdate,
    cast(columns[5] as char(15)) o_orderpriority,
    cast(columns[6] as char(15)) o_clerk,
    cast(columns[7] as int) o_shippriority,
    cast(columns[8] as varchar(79)) o_comment
from dfs.`/tpchRaw/SFScaleFactor/orders`;


create or replace  view lineitem_csvScaleFactor as select
    cast(columns[0] as bigint) l_orderkey,
    cast(columns[1] as bigint) l_partkey,
    cast(columns[2] as bigint) l_suppkey,
    cast(columns[3] as int) l_linenumber,
    cast(columns[4] as double) l_quantity,
    cast(columns[5] as double) l_extendedprice,
    cast(columns[6] as double) l_discount,
    cast(columns[7] as double) l_tax,
    cast(columns[8] as char(1)) l_returnflag,
    cast(columns[9] as char(1)) l_linestatus,
    cast(columns[10] as date) l_shipdate,
    cast(columns[11] as date) l_commitdate,
    cast(columns[12] as date) l_receiptdate,
    cast(columns[13] as char(25)) l_shipinstruct,
    cast(columns[14] as char(10)) l_shipmode,
    cast(columns[15] as varchar(44)) l_comment
from dfs.`/tpchRaw/SFScaleFactor/lineitem`;
