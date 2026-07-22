use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AnswerFact {
    pub fact: String,
    pub verified: bool,
    pub verified_by: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Foresight {
    pub foresight_text: String,
    pub relevant_from: String,
    pub relevant_until: String,
    pub duration_days: u32,
}
