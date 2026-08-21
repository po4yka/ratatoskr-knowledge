/// Deterministic source context supplied to one provider call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedContext {
    /// Version-one normalized source rendering.
    pub source: String,
    /// Block indexes included in provider-visible order.
    pub included_block_indexes: Vec<u32>,
    /// Complete tail-block indexes omitted from provider input.
    pub omitted_block_indexes: Vec<u32>,
    /// Configured Unicode character budget.
    pub character_budget: usize,
    /// Whether any complete tail block was omitted.
    pub truncated: bool,
}
