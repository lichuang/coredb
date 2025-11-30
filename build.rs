fn main() -> Result<(), Box<dyn std::error::Error>> {
  println!("cargo:rerun-if-changed=src/*");
  let mut config = prost_build::Config::new();
  config.protoc_arg("--experimental_allow_proto3_optional");
  let proto_files = [
    "src/types/proto/raft.proto",
    "src/types/proto/app_types.proto",
    "src/types/proto/app.proto",
  ];

  tonic_build::configure()
    .btree_map(["."])
    .type_attribute("openraftpb.Node", "#[derive(Eq)]")
    .type_attribute("openraftpb.SetRequest", "#[derive(Eq)]")
    .type_attribute("openraftpb.Response", "#[derive(Eq)]")
    .type_attribute("openraftpb.LeaderId", "#[derive(Eq)]")
    .type_attribute("openraftpb.Vote", "#[derive(Eq)]")
    .type_attribute("openraftpb.NodeIdSet", "#[derive(Eq)]")
    .type_attribute("openraftpb.Membership", "#[derive(Eq)]")
    .type_attribute("openraftpb.Entry", "#[derive(Eq)]")
    .compile_protos_with_config(config, &proto_files, &["proto"])?;
  Ok(())
}
