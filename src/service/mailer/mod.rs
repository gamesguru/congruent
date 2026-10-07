use std::sync::Arc;

use conduwuit::{Result, err, info};

use crate::{Args, client, mailer::messages::MessageTemplate};

pub mod messages;

pub struct Service {
	webhook: Option<Webhook>,
}

struct Webhook {
	client: reqwest::Client,
	url: String,
	sender: String,
	token: Option<String>,
}

#[derive(serde::Serialize)]
struct WebhookMessage<'a> {
	from: &'a str,
	to: &'a str,
	subject: &'a str,
	text: &'a str,
}

#[async_trait::async_trait]
impl crate::Service for Service {
	fn build(args: Args<'_>) -> Result<Arc<Self>> {
		let webhook = args.server.config.email.as_ref().map(|config| Webhook {
			client: args.require::<client::Service>("client").default.clone(),
			url: config.webhook_url.clone(),
			sender: config.sender.clone(),
			token: config.webhook_token.clone(),
		});

		Ok(Arc::new(Self { webhook }))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }

	async fn worker(self: Arc<Self>) -> Result<()> {
		if self.webhook.is_some() {
			info!("Email webhook is configured");
			Ok(())
		} else {
			info!("Email webhook is not configured, email functionality will be unavailable");
			Ok(())
		}
	}
}

impl Service {
	/// Returns a mailer which allows email to be sent, if the webhook is configured.
	#[must_use]
	pub fn mailer(&self) -> Option<Mailer<'_>> {
		self.webhook.as_ref().map(|webhook| Mailer { webhook })
	}

	pub fn expect_mailer(&self) -> Result<Mailer<'_>> {
		self.mailer().ok_or_else(|| {
			err!(Request(FeatureDisabled("This homeserver is not configured to send email.")))
		})
	}
}

pub struct Mailer<'a> {
	webhook: &'a Webhook,
}

impl Mailer<'_> {
	/// Sends an email.
	pub async fn send<Template: MessageTemplate>(
		&self,
		recipient: String,
		message: Template,
	) -> Result<()> {
		let subject = message.subject();
		let body = message.render();

		let payload = WebhookMessage {
			from: &self.webhook.sender,
			to: &recipient,
			subject: &subject,
			text: &body,
		};
		let mut request = self.webhook.client.post(&self.webhook.url).json(&payload);
		if let Some(token) = &self.webhook.token {
			request = request.bearer_auth(token);
		}
		request.send().await?.error_for_status()?;

		Ok(())
	}
}
