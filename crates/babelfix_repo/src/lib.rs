use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};

mod fix_orchestra;
pub mod fixify;

static FIX_ORCHESTRA: include_dir::Dir<'_> = include_dir::include_dir!(
  "$CARGO_MANIFEST_DIR/third-party/fix_orchestra/"
);

/// Error type for FIX repository parsing operations
#[derive(Debug, thiserror::Error)]
pub enum FixRepoError {
  #[error("IO error: {0}")]
  IoError(#[from] std::io::Error),

  #[error("XML error: {0}")]
  XmlError(#[from] quick_xml::Error),

  #[error("Parse error: {0}")]
  ParseError(String),
}

// Core data structures representing FIX metadata

/// Represents a FIX version (e.g., FIX.4.4, FIX.5.0SP2)
#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct FixVersion {
  pub name:         String,
  pub version:      String,
  pub fields:       HashMap<u32, Arc<Field>>,
  pub components:   HashMap<u32, Arc<Component>>,
  pub groups:       HashMap<u32, Arc<Group>>,
  pub messages:     HashMap<String, Arc<Message>>,
  pub codesets:     HashMap<String, Arc<Vec<EnumValue>>>,
  pub begin_string: String,
}

/// Represents a FIX field
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Field {
  pub id:         u32,
  pub name:       String,
  pub field_type: String,
}

impl Field {
  pub fn enum_values(
    &self,
    fix_repo: &FixVersion,
  ) -> Option<Arc<Vec<EnumValue>>> {
    fix_repo.codesets.get(&self.field_type).cloned()
  }
}

/// Represents an enumerated value for a field
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnumValue {
  pub value: String,
  pub name:  String,
  sort:      u32,
}

/// Reference to a field within a message, component, or group
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FieldRef {
  pub field_id: u32,
  pub required: bool,
}

/// Reference to a component within a message or another component
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ComponentRef {
  pub component_id: u32,
  pub required:     bool,
}

/// Reference to a group within a message or component
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GroupRef {
  pub group_id: u32,
  pub required: bool,
}

/// Represents an element in a message, component, or group
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MessageElement {
  Field(FieldRef),
  Component(ComponentRef),
  Group(GroupRef),
}

pub trait FieldBlock {
  fn get_elements(&self) -> &Vec<MessageElement>;
  fn get_members(&self) -> Option<&HashSet<u32>>;

  fn is_member(&self, _fix: &FixVersion, field: u32) -> bool {
    if let Some(members) = self.get_members() {
      return members.contains(&field);
    }
    // Fallback for types without a pre-computed member set (Message)
    self.is_member_slow(_fix, field)
  }

  fn is_member_slow(&self, fix: &FixVersion, field: u32) -> bool {
    for element in self.get_elements() {
      match element {
        MessageElement::Field(field_ref) => {
          if field_ref.field_id == field {
            return true;
          }
        }
        MessageElement::Component(comp_ref) => {
          if let Some(component) = fix.get_component(comp_ref.component_id) {
            if component.is_member(fix, field) {
              return true;
            }
          }
        }
        MessageElement::Group(group_ref) => {
          if let Some(group) = fix.get_group(group_ref.group_id) {
            // Don't consider group members as members
            if group.num_in_group_tag == field {
              return true;
            }
          }
        }
      }
    }
    false
  }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Block {
  Group(Group),
  Component(Component),
}

impl FieldBlock for Block {
  fn get_elements(&self) -> &Vec<MessageElement> {
    match self {
      Block::Group(group) => group.get_elements(),
      Block::Component(comp) => comp.get_elements(),
    }
  }
  fn get_members(&self) -> Option<&HashSet<u32>> {
    match self {
      Block::Group(group) => group.get_members(),
      Block::Component(comp) => comp.get_members(),
    }
  }
}

/// Represents a component (reusable block of fields)
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Component {
  pub id:       u32,
  pub name:     String,
  pub category: Option<String>,
  // Ordered collection of child elements (fields, components, groups)
  pub elements: Vec<MessageElement>,
  // Pre-computed set of all member field IDs (including nested components)
  pub members:  HashSet<u32>,
}

impl FieldBlock for Component {
  fn get_elements(&self) -> &Vec<MessageElement> {
    &self.elements
  }
  fn get_members(&self) -> Option<&HashSet<u32>> {
    Some(&self.members)
  }
}

impl Component {
}

/// Represents a message definition
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Message {
  pub name:        String,
  pub msg_type:    String,
  pub category:    Option<String>,
  pub description: Option<String>,
  // Ordered collection of child elements (fields, components, groups)
  pub elements:    Vec<MessageElement>,
}

impl Message {
}

impl FieldBlock for Message {
  fn get_elements(&self) -> &Vec<MessageElement> {
    &self.elements
  }
  fn get_members(&self) -> Option<&HashSet<u32>> {
    None // Messages don't need cached members
  }
}

/// Represents a repeating group
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Group {
  pub id:               u32,
  pub name:             String,
  pub required:         bool,
  pub num_in_group_tag: u32,
  // Ordered collection of child elements (fields, components, groups)
  pub elements:         Vec<MessageElement>,
  // Pre-computed set of all member field IDs (including nested components)
  pub members:          HashSet<u32>,
}

impl Group {
  pub fn get_marker_tag(&self, fix: &FixVersion) -> Arc<Field> {
    fn first_field_id(fix: &FixVersion, elements: &[MessageElement]) -> u32 {
      for element in elements {
        match element {
          MessageElement::Field(field_ref) => return field_ref.field_id,
          MessageElement::Component(comp_ref) => {
            if let Some(component) = fix.get_component(comp_ref.component_id) {
              return first_field_id(fix, &component.elements);
            }
          }
          MessageElement::Group(group_ref) => {
            if let Some(group) = fix.get_group(group_ref.group_id) {
              return group.num_in_group_tag;
            }
          }
        }
      }
      panic!("Group {} has no fields", elements.len())
    }
    fix.get_field(first_field_id(fix, &self.elements)).unwrap()
  }
}

impl FieldBlock for Group {
  fn get_elements(&self) -> &Vec<MessageElement> {
    &self.elements
  }
  fn get_members(&self) -> Option<&HashSet<u32>> {
    Some(&self.members)
  }
}

/// The main repository struct containing all FIX metadata
#[derive(Debug, Default, Eq, PartialEq)]
pub struct FixRepository {
  pub versions: HashMap<String, Arc<FixVersion>>,
}

impl FixVersion {
  /// Compute the member field sets for all components and groups.
  /// Must be called after all components/groups are loaded but before
  /// wrapping in Arc.
  pub fn compute_member_sets(&mut self) {
    // Compute component members
    let component_ids: Vec<u32> = self.components.keys().cloned().collect();
    for id in component_ids {
      let members = self.collect_members_for_component(id);
      if let Some(comp) = self.components.get_mut(&id) {
        Arc::make_mut(comp).members = members;
      }
    }

    // Compute group members
    let group_ids: Vec<u32> = self.groups.keys().cloned().collect();
    for id in group_ids {
      let members = self.collect_members_for_group(id);
      if let Some(group) = self.groups.get_mut(&id) {
        Arc::make_mut(group).members = members;
      }
    }
  }

  fn collect_members_for_component(&self, id: u32) -> HashSet<u32> {
    let mut members = HashSet::new();
    if let Some(comp) = self.components.get(&id) {
      self.collect_members_from_elements(&comp.elements, &mut members);
    }
    members
  }

  fn collect_members_for_group(&self, id: u32) -> HashSet<u32> {
    let mut members = HashSet::new();
    if let Some(group) = self.groups.get(&id) {
      // Include the num_in_group_tag itself
      members.insert(group.num_in_group_tag);
      self.collect_members_from_elements(&group.elements, &mut members);
    }
    members
  }

  fn collect_members_from_elements(
    &self,
    elements: &[MessageElement],
    members: &mut HashSet<u32>,
  ) {
    for element in elements {
      match element {
        MessageElement::Field(field_ref) => {
          members.insert(field_ref.field_id);
        }
        MessageElement::Component(comp_ref) => {
          if let Some(component) = self.components.get(&comp_ref.component_id) {
            self.collect_members_from_elements(&component.elements, members);
          }
        }
        MessageElement::Group(group_ref) => {
          // Only include the group's num_in_group_tag, not its members
          if let Some(group) = self.groups.get(&group_ref.group_id) {
            members.insert(group.num_in_group_tag);
          }
        }
      }
    }
  }
}

impl FixRepository {
  /// Create a new empty repository
  fn new() -> Self {
    Default::default()
  }

  pub fn get_version(&self, version: &str) -> Option<Arc<FixVersion>> {
    self.versions.get(version).cloned()
  }

  pub fn merge(mut self, other: FixRepository) -> Self {
    for (k, v) in other.versions {
      self.versions.insert(k, v);
    }
    self
  }
}

impl FixVersion {
  /// Get a field by ID
  pub fn get_field(&self, id: u32) -> Option<Arc<Field>> {
    self.fields.get(&id).cloned()
  }

  pub fn get_message(&self, name: &str) -> Option<Arc<Message>> {
    self.messages.get(name).cloned()
  }

  pub fn get_component(&self, id: u32) -> Option<Arc<Component>> {
    self.components.get(&id).cloned()
  }

  pub fn get_component_by_name(&self, name: &str) -> Option<Arc<Component>> {
    self.components.values().find(|c| c.name == name).cloned()
  }

  pub fn get_group(&self, group_id: u32) -> Option<Arc<Group>> {
    self.groups.get(&group_id).cloned()
  }

  pub fn get_group_by_name(&self, name: &str) -> Option<Arc<Group>> {
    self.groups.values().find(|g| g.name == name).cloned()
  }

  pub fn get_group_by_num_in_group_tag<T: FieldBlock>(
    &self,
    message: &T,
    num_in_group_tag: u32,
  ) -> Option<Arc<Group>> {
    for element in message.get_elements() {
      match element {
        MessageElement::Group(group) => {
          if let Some(group) = self.get_group(group.group_id) {
            if group.num_in_group_tag == num_in_group_tag {
              return Some(group);
            }
          }
        }
        MessageElement::Component(comp_ref) => {
          if let Some(component) = self.get_component(comp_ref.component_id) {
            if let Some(group) = self.get_group_by_num_in_group_tag(
              component.as_ref(),
              num_in_group_tag,
            ) {
              return Some(group);
            }
          }
        }
        MessageElement::Field(_) => {}
      }
    }
    None
  }

  /// Get ordered list of fields for a message, component, or group
  /// Returns a flat list of field references in document order
  pub fn get_fields<T: FieldBlock>(&self, block: &T) -> Vec<Arc<Field>> {
    let mut result = Vec::new();
    self.flatten_elements(block.get_elements(), &mut result, None);
    result
      .iter()
      .map(|r| self.fields.get(&r.field_id).unwrap().clone())
      .collect()
  }

  /// Helper method to flatten nested elements into a list of field references
  fn flatten_elements(
    &self,
    elements: &[MessageElement],
    field_refs: &mut Vec<FieldRef>,
    max_count: Option<usize>,
  ) {
    for element in elements {
      match element {
        MessageElement::Field(field_ref) => {
          field_refs.push(field_ref.clone());
          if let Some(max) = max_count {
            if field_refs.len() >= max {
              break;
            }
          }
        }
        MessageElement::Component(comp_ref) => {
          if let Some(component) = self.get_component(comp_ref.component_id) {
            self.flatten_elements(&component.elements, field_refs, max_count);
          }
        }
        MessageElement::Group(group_ref) => {
          if let Some(group) = self.get_group(group_ref.group_id) {
            // Add the NumInGroup field first
            field_refs.push(FieldRef {
              field_id: group.id,
              required: group.required,
            });

            if let Some(max) = max_count {
              if field_refs.len() >= max {
                break;
              }
            }

            // Then add the fields in the group
            self.flatten_elements(&group.elements, field_refs, max_count);
          }
        }
      }
    }
  }
}

// Public module interface functions
pub use fix_orchestra::{load_orchestration, load_orchestrations};

pub fn orchestrate() -> Result<FixRepository, FixRepoError> {
  let mut repo = FixRepository::new();
  for entry in FIX_ORCHESTRA.find("*.xml").map_err(|_| {
    FixRepoError::ParseError("Failed to find FIX Orchestra files".into())
  })? {
    let reader = std::io::BufReader::new(
      entry
        .as_file()
        .ok_or_else(|| {
          FixRepoError::ParseError("Failed to read FIX Orchestra file".into())
        })?
        .contents(),
    );
    repo = repo.merge(FixRepository::from_fix_orchestra(reader)?);
  }
  Ok(repo)
}

#[cfg(test)]
mod tests;
