#!/bin/bash
# 默认参数
USER=""
PASSWORD=""
LOG_FILE=""
PORT=""

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -U|--username)
            USER="$2"
            shift 2
            ;;
        -W|--password)
            PASSWORD="$2"
            shift 2
            ;;
        -o|--output)
            LOG_FILE="$2"
            shift 2
            ;;
        -p|--port)
            PORT="$2"
            shift 2
                ;;
        *)
            echo "未知参数: $1"
            echo "用法:sh $0 -U username -W password -p port -o output_file"
            exit 1
            ;;
    esac
done

# 检查必要参数
if [[ -z "$USER" || -z "$PASSWORD" || -z "$LOG_FILE" || -z "$PORT" ]]; then
    echo "错误: 缺少必要参数"
    echo "用法: sh $0 -U username -W password -p port -o output_file"
    exit 1
fi

# 覆盖写入文件开始
: > "$LOG_FILE"

# 获取所有数据库名称
databases=$(gsql -p $PORT -U $USER -W $PASSWORD -d postgres -c "SELECT datname FROM pg_database WHERE datname not in ('postgres','template0','template1', 'templatem','templatea');" | grep -v 'datname' | tail -n +2 | head -n -2)

# 检查数据库列表是否为空
if [[ -z "$databases" ]]; then
    echo "无可用的database" >> $LOG_FILE
    exit 0
fi

# 循环遍历每个数据库
for db in $databases; do
    # 去除首尾空白字符
    db=$(echo "$db" | xargs)

    echo "正在处理数据库: $db" >> $LOG_FILE

    # 连接到指定数据库并执行SQL查询
    gsql -p $PORT -U $USER -W $PASSWORD -d $db -c " 
	WITH partition_info AS 
		(SELECT n.nspname AS schema_name,
			 c.relname AS table_name,
			 c.oid AS table_oid,
			 COALESCE(pp.relpages,
			p.relpages) AS relpages,
			 COALESCE(pp.reltuples,
			p.reltuples) AS reltuples
		FROM pg_partition p
		JOIN pg_class c
			ON c.oid = p.parentid
				AND c.parttype IN ('p','s')
		JOIN pg_namespace n
			ON n.oid = c.relnamespace
		LEFT JOIN pg_partition pp
			ON pp.parentid = p.oid
				AND pp.parttype = 's'
		WHERE p.parttype='p' )
	SELECT *,
		 pg_stat_get_last_vacuum_time(table_oid) AS last_vacuum,
		 pg_stat_get_last_autovacuum_time(table_oid) AS last_autovacuum,
		 pg_stat_get_last_analyze_time(table_oid) AS last_analyze,
		 pg_stat_get_last_autoanalyze_time(table_oid) AS last_autoanalyze
	FROM 
		(SELECT table_oid,
			a.schema_name,
			a.table_name,
			pg_relation_size(table_oid)/1024 AS size,
			 a.pages*8.192 AS pagesize,
			a.reltuples
		FROM 
			(SELECT schema_name,
			 table_name,
			table_oid,
			 sum(relpages) AS pages,
			sum(reltuples) AS reltuples
			FROM partition_info
			GROUP BY  schema_name, table_name,table_oid) a )
		WHERE size > 1000
			AND pagesize * 1000 < 120*size;
    " >> $LOG_FILE 2>&1 

    if [[ $? -ne 0 ]]; then
        echo -e "${RED}错误: 查询数据库 $db 失败${NC}" | tee -a "$LOG_FILE"
        exit 1
    fi
    echo "完成数据库: $db 的处理" >> $LOG_FILE
done
