use super::*;

fn project_root() -> std::path::PathBuf {
  let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
    .unwrap_or("crates/babelfix_repo".into());
  std::path::PathBuf::from(manifest_dir)
}

fn print_version(fix: &FixVersion) {
  println!("Version: {}", fix.name);
  println!("Messages:");
  for msg in fix.messages.values() {
    println!("  {}: {}", msg.msg_type, msg.name);
  }
  println!("Fields:");
  for field in fix.fields.values() {
    println!("  {}: {}", field.id, field.name);
    if let Some(enum_values) = field.enum_values(fix) {
      for enum_value in enum_values.iter() {
        println!("    {} ({})", enum_value.value, enum_value.name);
      }
    }
  }
  println!("Components:");
  for comp in fix.components.values() {
    println!("  {}: {}", comp.id, comp.name);
  }
  println!("Groups:");
  for group in fix.groups.values() {
    println!("  {}: {}", group.id, group.name);
  }
}

fn print_elements(fix: &FixVersion, element: &impl FieldBlock, depth: usize) {
  for e in element.get_elements() {
    match e {
      MessageElement::Field(field_ref) => {
        let field = fix.get_field(field_ref.field_id).unwrap();
        println!("{}Field: {}/{}", "  ".repeat(depth), field.id, field.name,);
        if let Some(enum_values) = field.enum_values(fix) {
          for enum_value in enum_values.iter() {
            println!(
              "{}Enum: {} ({})",
              "  ".repeat(depth + 2),
              enum_value.value,
              enum_value.name
            );
          }
        }
      }
      MessageElement::Component(comp_ref) => {
        let component = fix.get_component(comp_ref.component_id).unwrap();
        println!(
          "{}ComponentRef: {}/{}",
          "  ".repeat(depth),
          component.id,
          component.name,
        );
        print_elements(fix, component.as_ref(), depth + 2);
      }
      MessageElement::Group(group_ref) => {
        let group = fix.get_group(group_ref.group_id).unwrap();
        println!(
          "{}Group: {}/{}",
          "  ".repeat(depth),
          group.id,
          fix.get_group(group.id).unwrap().name
        );
        print_elements(fix, group.as_ref(), depth + 1);
      }
    }
  }
}

#[test]
fn test_full_fix_orchestra() {
  let repo = load_orchestrations(vec![
    project_root().join("third-party/fix_orchestra/OrchestraFIX42.xml"),
    project_root().join("third-party/fix_orchestra/OrchestraFIX44.xml"),
    project_root().join("third-party/fix_orchestra/OrchestraFIXLatest.xml"),
  ])
  .unwrap();

  let _fix42 = repo.get_version("FIX.4.2").unwrap();
  let fix44 = repo.get_version("FIX.4.4").unwrap();
  let fix_latest = repo.get_version("FIX.Latest").unwrap();

  println!("==> Fix Latest");
  print_version(&fix_latest);

  // FIX5
  let new_order_single = fix_latest.get_message("D").unwrap();
  println!("==> FIX5.0 NewOrderSingle");
  print_elements(&fix_latest, &*new_order_single, 0);
  assert!(!new_order_single.is_member(&fix_latest, 555));

  let new_order_multi_leg = fix_latest.get_message("AB").unwrap();
  println!("==> FIX5.0 NewOrderMultiLeg");
  print_elements(&fix_latest, &*new_order_multi_leg, 0);
  let instrument_leg_grp =
    fix_latest.get_group_by_name("InstrmtLegGrp").unwrap();
  assert!(instrument_leg_grp.is_member(&fix_latest, 600));
  let instrument_leg_grp = fix_latest
    .get_group_by_num_in_group_tag(&*new_order_multi_leg, 555)
    .unwrap();
  assert!(instrument_leg_grp.is_member(&fix_latest, 600));

  // FIX44
  let new_order_single = fix44.get_message("D").unwrap();
  println!("==> FIX.4.4 NewOrderSingle");
  print_elements(&fix44, &*new_order_single, 0);
  assert!(!new_order_single.is_member(&fix44, 555));
  assert!(new_order_single.is_member(&fix44, 100))
}

#[test]
fn test_full_fix_orchestrate() {
  let repo = orchestrate().unwrap();

  let _fix42 = repo.get_version("FIX.4.2").unwrap();
  let fix44 = repo.get_version("FIX.4.4").unwrap();
  let fix_latest = repo.get_version("FIX.Latest").unwrap();

  println!("==> Fix Latest");
  print_version(&fix_latest);

  // FIX5
  let new_order_single = fix_latest.get_message("D").unwrap();
  println!("==> FIX5.0 NewOrderSingle");
  print_elements(&fix_latest, &*new_order_single, 0);
  assert!(!new_order_single.is_member(&fix_latest, 555));

  let new_order_multi_leg = fix_latest.get_message("AB").unwrap();
  println!("==> FIX5.0 NewOrderMultiLeg");
  print_elements(&fix_latest, &*new_order_multi_leg, 0);
  let instrument_leg_grp =
    fix_latest.get_group_by_name("InstrmtLegGrp").unwrap();
  assert!(instrument_leg_grp.is_member(&fix_latest, 600));
  let instrument_leg_grp = fix_latest
    .get_group_by_num_in_group_tag(&*new_order_multi_leg, 555)
    .unwrap();
  assert!(instrument_leg_grp.is_member(&fix_latest, 600));

  // FIX44
  let new_order_single = fix44.get_message("D").unwrap();
  println!("==> FIX.4.4 NewOrderSingle");
  print_elements(&fix44, &*new_order_single, 0);
  assert!(!new_order_single.is_member(&fix44, 555));
  assert!(new_order_single.is_member(&fix44, 100))
}
