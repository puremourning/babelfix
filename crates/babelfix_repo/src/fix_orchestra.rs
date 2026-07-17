use std::fs::File;
use std::io::BufReader;

use super::*;

pub fn load_orchestrations<P: AsRef<Path>>(
  paths: impl IntoIterator<Item = P>,
) -> Result<FixRepository, FixRepoError> {
  let mut repo = FixRepository::new();
  for p in paths {
    repo = repo.merge(load_orchestration(p)?);
  }
  Ok(repo)
}

/// Load a FIX repository from an XML file
pub fn load_orchestration<P: AsRef<Path>>(
  path: P,
) -> Result<FixRepository, FixRepoError> {
  FixRepository::from_fix_orchestra_xml(path)
}

impl FixRepository {
  pub fn from_fix_orchestra_xml<P: AsRef<Path>>(
    path: P,
  ) -> Result<Self, FixRepoError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    Self::from_fix_orchestra(reader)
  }

  pub fn from_fix_orchestra<R: std::io::BufRead>(
    reader: R,
  ) -> Result<Self, FixRepoError> {
    let mut parser = FixOrchestraParser::default();
    parser.parse(reader)
  }
}

/// Parser for the FIX repository XML
#[derive(Debug, Default)]
struct FixOrchestraParser {
  repository: FixRepository,
  current_fix_version: FixVersion,
  current_field: Option<Field>,
  current_message: Option<Message>,
  current_group: Option<Group>,
  current_codeset: Option<(String, Vec<EnumValue>)>,
  block_stack: Vec<Block>,
  buffer: String,
}

impl FixOrchestraParser {
  fn parse<R: std::io::BufRead>(
    &mut self,
    reader: R,
  ) -> Result<FixRepository, FixRepoError> {
    let mut xml_reader = Reader::from_reader(reader);
    xml_reader.config_mut().trim_text(true);
    let mut buf = Vec::new();

    loop {
      match xml_reader.read_event_into(&mut buf) {
        Ok(Event::Start(ref e)) => {
          let name = String::from_utf8_lossy(e.name().into_inner());
          self.handle_start_element(name.as_ref(), e)?;
        }
        Ok(Event::End(ref e)) => {
          let name = String::from_utf8_lossy(e.name().into_inner());
          self.handle_end_element(name.as_ref())?;
        }
        Ok(Event::Empty(ref e)) => {
          let name = String::from_utf8_lossy(e.name().into_inner());
          self.handle_start_element(name.as_ref(), e)?;
          self.handle_end_element(name.as_ref())?;
        }
        Ok(Event::Text(e)) => {
          self.buffer = e.unescape()?.to_string();
        }
        Ok(Event::Eof) => break,
        Err(e) => return Err(FixRepoError::XmlError(e)),
        _ => (),
      }
    }
    println!("Loaded {} FIX versions", self.repository.versions.len());
    Ok(std::mem::take(&mut self.repository))
  }

  fn handle_start_element(
    &mut self,
    name: &str,
    e: &BytesStart,
  ) -> Result<(), FixRepoError> {
    match name {
      "fixr:repository" => {
        let attrs = self.get_attributes(e);
        let version = attrs.get("version").cloned().unwrap();
        self.current_fix_version = FixVersion {
          name: attrs.get("name").cloned().unwrap(),
          // In FIX Orchestra-land, there are only FIX 4.2, FIX 4.4 and FIX 5.0
          // (various), which uses FIXT session protocol
          begin_string: match version.as_str() {
            "FIX.4.2" => "FIX.4.2".to_string(),
            "FIX.4.4" => "FIX.4.4".to_string(),
            _ => "FIXT.1.1".to_string(),
          },
          version,
          ..Default::default()
        }
      }
      "fixr:field" => {
        let attrs = self.get_attributes(e);
        let id = attrs
          .get("id")
          .and_then(|id_str| id_str.parse().ok())
          .unwrap();

        self.current_field = Some(Field {
          id,
          name: attrs.get("name").cloned().unwrap(),
          field_type: attrs.get("type").cloned().unwrap(),
        });
      }
      "fixr:codeSet" => {
        let attrs = self.get_attributes(e);
        self.current_codeset =
          Some((attrs.get("name").cloned().unwrap(), Vec::new()));
      }
      "fixr:code" => {
        let attrs = self.get_attributes(e);
        let enum_value = EnumValue {
          value: attrs.get("value").cloned().unwrap(),
          name: attrs.get("name").cloned().unwrap(),
          sort: attrs.get("sort").and_then(|s| s.parse().ok()).unwrap_or(0),
        };
        let Some(ref mut current_codeset) = self.current_codeset else {
          return Err(FixRepoError::ParseError(
            "Code without a codeset".to_string(),
          ));
        };
        current_codeset.1.push(enum_value);
      }
      "fixr:component" => {
        let attrs = self.get_attributes(e);
        let id = attrs
          .get("id")
          .and_then(|id_str| id_str.parse().ok())
          .unwrap();
        let name = attrs.get("name").cloned().unwrap();
        self.block_stack.push(Block::Component(Component {
          id,
          name,
          category: attrs.get("category").cloned(),
          elements: Vec::new(),
          members: HashSet::new(),
        }));
      }
      "fixr:group" => {
        let attrs = self.get_attributes(e);
        // OK this gets a little tricky.
        // In the file we have
        //   Group [ id = groupID, name = ... ]
        //      -> NumInGroup [ id = 555 ]
        //      -> FieldRef ...
        //      -> ComponentRef ...
        //      -> GroupRef [ id = some groupID,  ...
        let id = attrs
          .get("id")
          .and_then(|id_str| id_str.parse().ok())
          .unwrap();
        let name = attrs.get("name").cloned().unwrap();
        self.current_group = Some(Group {
          id,
          num_in_group_tag: 0, // Will fill in when we see it
          name,
          elements: Vec::new(),
          required: false,
          members: HashSet::new(),
        });
      }
      "fixr:numInGroup" => {
        let attrs = self.get_attributes(e);
        if let Some(mut group) = self.current_group.take() {
          group.num_in_group_tag = attrs
            .get("id")
            .and_then(|id_str| id_str.parse().ok())
            .unwrap();
          self.block_stack.push(Block::Group(group));
        } else {
          return Err(FixRepoError::ParseError(
            "NumInGroup without a group".to_string(),
          ));
        }
      }
      "fixr:message" => {
        let attrs = self.get_attributes(e);
        let name = attrs.get("name").cloned().unwrap();

        self.current_message = Some(Message {
          name: name.clone(),
          msg_type: attrs.get("msgType").cloned().unwrap(),
          category: attrs.get("category").cloned(),
          description: None,
          elements: Vec::new(),
        });
      }

      "fixr:fieldRef" => {
        let attrs = self.get_attributes(e);
        let id = attrs
          .get("id")
          .and_then(|id_str| id_str.parse().ok())
          .unwrap();
        let required = attrs.get("presence").is_some_and(|p| p == "required");

        let field_ref = FieldRef {
          field_id: id,
          required,
        };

        let element = MessageElement::Field(field_ref);

        match self.block_stack.last_mut() {
          Some(Block::Group(group)) => {
            group.elements.push(element);
          }
          Some(Block::Component(component)) => {
            component.elements.push(element);
          }
          _ => {
            if let Some(message) = &mut self.current_message {
              message.elements.push(element);
            } else {
              return Err(FixRepoError::ParseError(
                "FieldRef without a group, component, or message".to_string(),
              ));
            }
          }
        }
      }
      "fixr:groupRef" => {
        let attrs = self.get_attributes(e);
        let id = attrs
          .get("id")
          .and_then(|id_str| id_str.parse().ok())
          .unwrap();
        let required = attrs.get("presence").is_some_and(|p| p == "required");

        let group_ref = GroupRef {
          group_id: id,
          required,
        };

        let element = MessageElement::Group(group_ref);

        match self.block_stack.last_mut() {
          Some(Block::Group(group)) => {
            group.elements.push(element);
          }
          Some(Block::Component(component)) => {
            component.elements.push(element);
          }
          _ => {
            if let Some(message) = &mut self.current_message {
              message.elements.push(element);
            } else {
              return Err(FixRepoError::ParseError(
                "GroupRef without a group, component, or message".to_string(),
              ));
            }
          }
        }
      }
      "fixr:componentRef" => {
        let attrs = self.get_attributes(e);
        let id = attrs
          .get("id")
          .and_then(|id_str| id_str.parse().ok())
          .unwrap();
        let required = attrs.get("presence").is_some_and(|p| p == "required");

        let component_ref = ComponentRef {
          component_id: id,
          required,
        };

        let element = MessageElement::Component(component_ref);

        match self.block_stack.last_mut() {
          Some(Block::Group(group)) => {
            group.elements.push(element);
          }
          Some(Block::Component(component)) => {
            component.elements.push(element);
          }
          _ => {
            if let Some(message) = &mut self.current_message {
              message.elements.push(element);
            } else {
              return Err(FixRepoError::ParseError(
                "ComponentRef without a group, component, or message"
                  .to_string(),
              ));
            }
          }
        }
      }
      _ => {}
    }
    Ok(())
  }

  fn handle_end_element(&mut self, name: &str) -> Result<(), FixRepoError> {
    match name {
      "fixr:repository" => {
        let mut version = std::mem::take(&mut self.current_fix_version);
        version.compute_member_sets();
        self
          .repository
          .versions
          .insert(version.name.clone(), Arc::new(version));
      }
      "fixr:field" => {
        if let Some(field) = self.current_field.take() {
          self
            .current_fix_version
            .fields
            .insert(field.id, Arc::new(field));
        } else {
          return Err(FixRepoError::ParseError(
            "Unexpected end of field".to_string(),
          ));
        }
      }
      "fixr:codeSet" => {
        let Some((name, mut enum_values)) = self.current_codeset.take() else {
          return Err(FixRepoError::ParseError(
            "Unexpected end of codeset".to_string(),
          ));
        };
        enum_values.sort_by_key(|a| a.sort);
        self
          .current_fix_version
          .codesets
          .insert(name, Arc::new(enum_values));
      }
      "fixr:group" => {
        if let Some(Block::Group(group)) = self.block_stack.pop() {
          self
            .current_fix_version
            .groups
            .insert(group.id, Arc::new(group));
        } else {
          return Err(FixRepoError::ParseError(
            "Unexpected end of group".to_string(),
          ));
        }
      }
      "fixr:component" => {
        if let Some(Block::Component(component)) = self.block_stack.pop() {
          self
            .current_fix_version
            .components
            .insert(component.id, Arc::new(component));
        } else {
          return Err(FixRepoError::ParseError(
            "Unexpected end of component".to_string(),
          ));
        }
      }
      "fixr:message" => {
        if let Some(message) = self.current_message.take() {
          self
            .current_fix_version
            .messages
            .insert(message.msg_type.clone(), Arc::new(message));
        } else {
          return Err(FixRepoError::ParseError(
            "Unexpected end of message".to_string(),
          ));
        }
      }
      _ => {}
    }
    Ok(())
  }

  fn get_attributes(&self, e: &BytesStart) -> HashMap<String, String> {
    let mut attrs = HashMap::new();
    for attr in e.attributes().flatten() {
      let key = String::from_utf8_lossy(attr.key.into_inner()).to_string();
      let value = attr.unescape_value().unwrap_or_default().to_string();
      attrs.insert(key, value);
    }
    attrs
  }
}
