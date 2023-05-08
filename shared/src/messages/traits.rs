pub trait Message {
    fn type_name() -> &'static str;
}
