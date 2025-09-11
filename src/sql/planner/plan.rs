use std::collections::HashMap;
use serde::{Deserialize, Serialize};
/// 执行计划
///
/// 计划的根节点指定要执行的操作（例如 SELECT、INSERT、UPDATE 等）。
/// 它包含一个嵌套的子节点树，用于流式处理和处理行。
///
/// 下面是一个（未优化的）查询计划示例：
///
/// SELECT title, released, genres.name AS genre
/// FROM movies INNER JOIN genres ON movies.genre_id = genres.id
/// WHERE released >= 2000
/// ORDER BY released
///
/// Select
/// └─ Order: movies.released desc
///    └─ Projection: movies.title, movies.released, genres.name as genre
///       └─ Filter: movies.released >= 2000
///          └─ NestedLoopJoin: inner on movies.genre_id = genres.id
///             ├─ Scan: movies
///             └─ Scan: genres
///
/// 行从树的叶子节点流向根节点：
///
/// 1. Scan 节点从 movies 和 genres 读取行。
/// 2. NestedLoopJoin 将 movies 和 genres 的行进行连接。
/// 3. Filter 丢弃发行日期早于 2000 年的行。
/// 4. Projection 从行中挑选出请求的列值。
/// 5. Order 根据发行日期对行进行排序。
/// 6. Select 将最终的行返回给客户端。
use crate::types::*;
#[derive(Clone,Debug,PartialEq,Deserialize,Serialize)]
pub enum Plan {
    CreateTable { schema: Table },

    DropTable { name: String, if_exists: bool },

    Delete { table: String, primary_key: usize , source: Node },

    Insert { table: Table, column_map: Option<HashMap<usize,usize>>, source: Node },

    Update { table: Table, primary_key: usize, source: Node, expressions: Vec<(usize,Expression)> },

    Select(Node)
}


#[derive(Clone,Debug,PartialEq,Deserialize,Serialize)]
pub enum Node {}