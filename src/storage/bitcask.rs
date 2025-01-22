/// 实现一个BitCask结构
/// struct - BitCask
/// 成员：
/// 日志文件目录 - Log
/// 全局的映射表 - KeyDir
pub struct BitCask {
    log: Log,
    keydir: KeyDir,
}
/// KeyDir
/// 维护key和（fileId、value_sz、value_pos、tstamp）的映射关系
type KeyDir = std::collections::BTreeMap<Vec<u8>, (u8, u32)>;

struct Log {}
