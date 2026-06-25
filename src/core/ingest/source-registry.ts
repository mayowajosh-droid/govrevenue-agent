export type SourceCategory =
  | "procurement_contracts"
  | "government_public"
  | "transport_infrastructure"
  | "health_education_social"
  | "environment_energy"
  | "news_media"
  | "crime_justice"
  | "consumer_demand"
  | "market_intelligence"
  | "regulatory_directories"
  | "creative_cultural"
  | "devolved_nations"
  | "other";

export type UpdateCadence = "realtime" | "daily" | "weekly" | "monthly";

export type DataSource = {
  id: string;
  name: string;
  category: SourceCategory;
  cadence: UpdateCadence;
  requiresKey: boolean;
  keyEnvVar?: string;
  live: boolean;
  description: string;
  baseUrl: string;
};

export const DATA_SOURCES: DataSource[] = [
  // PROCUREMENT & CONTRACTS (5)
  {
    id: "contracts_finder",
    name: "Contracts Finder",
    category: "procurement_contracts",
    cadence: "realtime",
    requiresKey: false,
    live: true,
    description: "UK public-sector contracts above £12k. Primary procurement data source.",
    baseUrl: "https://www.contractsfinder.service.gov.uk/Published/Notices/PublishedNoticeList/Search",
  },
  {
    id: "find_a_tender",
    name: "Find a Tender Service (FTS)",
    category: "procurement_contracts",
    cadence: "realtime",
    requiresKey: false,
    live: true,
    description: "Above-threshold OJEU replacement notices. Cabinet Office.",
    baseUrl: "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages",
  },
  {
    id: "sell2wales",
    name: "Sell2Wales",
    category: "procurement_contracts",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Welsh Government procurement portal. RSS feed.",
    baseUrl: "https://www.sell2wales.gov.wales/rss/rss_switch.aspx",
  },
  {
    id: "nhsbsa_pipeline",
    name: "NHSBSA Pipeline (CKAN)",
    category: "procurement_contracts",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "NHS Business Services Authority open data and procurement pipeline.",
    baseUrl: "https://opendata.nhsbsa.net/api/3/action",
  },
  {
    id: "gazette",
    name: "The Gazette",
    category: "procurement_contracts",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Official UK government gazette — insolvency, company, and procurement notices.",
    baseUrl: "https://www.thegazette.co.uk",
  },

  // GOVERNMENT & PUBLIC DATA (10)
  {
    id: "ons",
    name: "ONS API",
    category: "government_public",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "Office for National Statistics — construction output, business demography, GDP.",
    baseUrl: "https://api.ons.gov.uk",
  },
  {
    id: "nomis",
    name: "Nomis API",
    category: "government_public",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "ONS labour market data — employment, business counts, BRES by sector and area.",
    baseUrl: "https://www.nomisweb.co.uk/api/v01",
  },
  {
    id: "companies_house",
    name: "Companies House",
    category: "government_public",
    cadence: "daily",
    requiresKey: true,
    keyEnvVar: "COMPANIES_HOUSE_API_KEY",
    live: true,
    description: "Company registration, officers, PSCs, filing history.",
    baseUrl: "https://api.company-information.service.gov.uk",
  },
  {
    id: "charity_commission",
    name: "Charity Commission",
    category: "government_public",
    cadence: "weekly",
    requiresKey: true,
    keyEnvVar: "CHARITY_COMMISSION_API_KEY",
    live: true,
    description: "England & Wales charity register — income, status, activities.",
    baseUrl: "https://api.charitycommission.gov.uk/register/api",
  },
  {
    id: "uk_parliament",
    name: "UK Parliament API",
    category: "government_public",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Written questions, committee reports, Hansard debates.",
    baseUrl: "https://questions-statements-api.parliament.uk/api",
  },
  {
    id: "govuk_content",
    name: "GOV.UK Content API",
    category: "government_public",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Policy publications, consultations, spending reviews, strategies.",
    baseUrl: "https://www.gov.uk/api/search.json",
  },
  {
    id: "planning_data",
    name: "Planning Data API (DLUHC)",
    category: "government_public",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Brownfield land, conservation areas, Article 4 directions.",
    baseUrl: "https://www.planning.data.gov.uk/api/v1",
  },
  {
    id: "land_registry",
    name: "HM Land Registry (HMLR)",
    category: "government_public",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "Property transaction prices by district. Price Paid Data API.",
    baseUrl: "https://landregistry.data.gov.uk/data/ppi",
  },
  {
    id: "local_authority_spending",
    name: "Local Authority Spending (data.gov.uk)",
    category: "government_public",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "Local authority spending over £500 datasets indexed on data.gov.uk.",
    baseUrl: "https://data.gov.uk/api/action",
  },
  {
    id: "data_gov_uk",
    name: "data.gov.uk CKAN API",
    category: "government_public",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "UK open government data — CKAN-based search across all public datasets.",
    baseUrl: "https://data.gov.uk/api/action",
  },

  // TRANSPORT & INFRASTRUCTURE (6)
  {
    id: "tfl",
    name: "TfL Unified API",
    category: "transport_infrastructure",
    cadence: "realtime",
    requiresKey: false,
    live: true,
    description: "Transport for London — line statuses, stop points, disruptions.",
    baseUrl: "https://api.tfl.gov.uk",
  },
  {
    id: "rail_data",
    name: "Rail Data Marketplace (ORR)",
    category: "transport_infrastructure",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "Office of Rail and Road — station usage, performance statistics.",
    baseUrl: "https://dataportal.orr.gov.uk",
  },
  {
    id: "bus_open_data",
    name: "Bus Open Data Service",
    category: "transport_infrastructure",
    cadence: "daily",
    requiresKey: true,
    keyEnvVar: "BODS_API_KEY",
    live: true,
    description: "DfT Bus Open Data Service — operators, datasets, timetables.",
    baseUrl: "https://data.bus-data.dft.gov.uk/api/v1",
  },
  {
    id: "networkrail",
    name: "NetworkRail Open Data",
    category: "transport_infrastructure",
    cadence: "daily",
    requiresKey: true,
    keyEnvVar: "NETWORK_RAIL_TOKEN",
    live: true,
    description: "NetworkRail data feeds — train running data, schedule, assets.",
    baseUrl: "https://datafeeds.networkrail.co.uk",
  },
  {
    id: "os_names",
    name: "OS Names API",
    category: "transport_infrastructure",
    cadence: "daily",
    requiresKey: true,
    keyEnvVar: "OS_API_KEY",
    live: true,
    description: "Ordnance Survey place name search — settlements, roads, geography.",
    baseUrl: "https://api.os.uk/search/names/v1",
  },
  {
    id: "postcodes_io",
    name: "Postcodes.io",
    category: "transport_infrastructure",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Free UK postcode lookup — district, constituency, lat/lon, NHS area.",
    baseUrl: "https://api.postcodes.io",
  },

  // HEALTH, EDUCATION & SOCIAL (6)
  {
    id: "nhs_content",
    name: "NHS ODS (Organisation Data Service)",
    category: "health_education_social",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "NHS organisational directory — trusts, CCGs, GP practices.",
    baseUrl: "https://directory.spineservices.nhs.uk/ORD/2-0-0",
  },
  {
    id: "food_hygiene",
    name: "Food Hygiene Ratings API (FSA)",
    category: "health_education_social",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "Food Standards Agency ratings for catering/hospitality sector intel.",
    baseUrl: "https://ratings.food.gov.uk",
  },
  {
    id: "explore_education",
    name: "Explore Education (DfE)",
    category: "health_education_social",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "DfE school information — establishments, types, local authority.",
    baseUrl: "https://get-information-schools.service.gov.uk",
  },
  {
    id: "unistats",
    name: "Unistats / Discover Uni",
    category: "health_education_social",
    cadence: "monthly",
    requiresKey: false,
    live: false,
    description: "Higher education course data. Public API deprecated; data via HESA.",
    baseUrl: "https://discoveruni.gov.uk",
  },
  {
    id: "three_sixty_giving",
    name: "360Giving GrantNav",
    category: "health_education_social",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "UK grant-making data — funding organisations, recipients, amounts.",
    baseUrl: "https://grantnav.threesixtygiving.org/api",
  },
  {
    id: "find_that_charity",
    name: "Find That Charity (FTC)",
    category: "health_education_social",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "Charity search aggregator — CH numbers, income, registration data.",
    baseUrl: "https://findthatcharity.uk",
  },

  // ENVIRONMENT & ENERGY (5)
  {
    id: "met_office",
    name: "Met Office DataHub",
    category: "environment_energy",
    cadence: "daily",
    requiresKey: true,
    keyEnvVar: "MET_OFFICE_API_KEY",
    live: true,
    description: "Weather forecasts and warnings — relevant for construction/energy desks.",
    baseUrl: "https://data.hub.api.metoffice.gov.uk",
  },
  {
    id: "environment_agency",
    name: "Environment Agency APIs",
    category: "environment_energy",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Flood warnings, monitoring stations, water quality data.",
    baseUrl: "https://environment.data.gov.uk/flood-monitoring",
  },
  {
    id: "defra",
    name: "Defra Data Services",
    category: "environment_energy",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "Environmental permits, waste data, air quality, agricultural statistics.",
    baseUrl: "https://data.gov.uk",
  },
  {
    id: "octopus_energy",
    name: "Octopus Energy API",
    category: "environment_energy",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Energy product and tariff data — green energy, variable rates.",
    baseUrl: "https://api.octopus.energy/v1",
  },
  {
    id: "natural_resources_wales",
    name: "Natural Resources Wales APIs",
    category: "environment_energy",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "NRW open data — water quality, forestry, biodiversity.",
    baseUrl: "https://data.gov.uk",
  },

  // NEWS & MEDIA (1)
  {
    id: "bbc_news",
    name: "BBC News Labs APIs",
    category: "news_media",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "BBC News RSS feeds — business, politics, UK news for signal context.",
    baseUrl: "https://feeds.bbci.co.uk/news",
  },

  // CRIME & JUSTICE (2)
  {
    id: "uk_police",
    name: "UK Police API",
    category: "crime_justice",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "Crime data by location and force — contextual intel for security/facilities desks.",
    baseUrl: "https://data.police.uk/api",
  },
  {
    id: "courts_tribunals",
    name: "Courts & Tribunals API",
    category: "crime_justice",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "Court finder — areas of law, court types, locations.",
    baseUrl: "https://www.find-court-tribunal.service.gov.uk",
  },

  // OTHER (2)
  {
    id: "hansard",
    name: "Hansard API",
    category: "other",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Parliamentary debates — procurement, construction, health, energy mentions.",
    baseUrl: "https://hansard-api.parliament.uk",
  },
  {
    id: "energy_tech_list",
    name: "ETL API (Energy Tech List)",
    category: "other",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "DESNZ approved energy-efficient product list. Indexed via data.gov.uk.",
    baseUrl: "https://etl.beis.gov.uk",
  },

  // ── CONSUMER DEMAND ──────────────────────────────────────────────────────
  {
    id: "wikipedia_pageviews",
    name: "Wikipedia Pageviews API",
    category: "consumer_demand",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Top 200 most-viewed English Wikipedia articles daily. Topic interest proxy — spikes signal emerging cultural demand before it hits search trends.",
    baseUrl: "https://wikimedia.org/api/rest_v1/metrics/pageviews",
  },
  {
    id: "reddit_signals",
    name: "Reddit Public JSON API",
    category: "consumer_demand",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Hot and rising posts from UK business, finance, and lifestyle subreddits. Community-level demand signals before they reach mainstream media.",
    baseUrl: "https://www.reddit.com",
  },
  {
    id: "eventbrite",
    name: "Eventbrite Events API",
    category: "consumer_demand",
    cadence: "daily",
    requiresKey: true,
    keyEnvVar: "EVENTBRITE_API_KEY",
    live: true,
    description: "UK events — concerts, markets, festivals, trade shows, fitness classes. Demand concentration signals for catering, fashion, entertainment, and hospitality.",
    baseUrl: "https://www.eventbriteapi.com/v3",
  },
  {
    id: "youtube_data",
    name: "YouTube Data API v3",
    category: "consumer_demand",
    cadence: "daily",
    requiresKey: true,
    keyEnvVar: "YOUTUBE_API_KEY",
    live: true,
    description: "UK trending videos by category and view count. Content creator and entertainment demand signals. Free Google Cloud API key required.",
    baseUrl: "https://www.googleapis.com/youtube/v3",
  },
  {
    id: "spotify_api",
    name: "Spotify Web API",
    category: "consumer_demand",
    cadence: "daily",
    requiresKey: true,
    keyEnvVar: "SPOTIFY_CLIENT_ID",
    live: true,
    description: "UK music categories, featured playlists, trending genres. Cultural demand signals for events, lifestyle brands, and entertainment businesses.",
    baseUrl: "https://api.spotify.com/v1",
  },

  // ── MARKET INTELLIGENCE ──────────────────────────────────────────────────
  {
    id: "ukri_grants",
    name: "UKRI Gateway to Research",
    category: "market_intelligence",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "UK Research & Innovation funded projects — innovation, science, and tech grant data. Signals emerging sectors before they become commercial markets.",
    baseUrl: "https://gtr.ukri.org/gtr/api",
  },
  {
    id: "ukri_innovate",
    name: "Innovate UK Grants (UKRI)",
    category: "market_intelligence",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "Innovate UK specific project funding — technology commercialisation signals. Identifies which sectors government is backing for near-term growth.",
    baseUrl: "https://gtr.ukri.org/gtr/api",
  },
  {
    id: "hmrc_trade",
    name: "HMRC UK Trade Info (OData)",
    category: "market_intelligence",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "UK import/export flows by commodity code. Demand signals for manufacturers, wholesalers, importers, and any business with supply-chain exposure.",
    baseUrl: "https://www.uktradeinfo.com/api/odata",
  },
  {
    id: "ipo_trademarks",
    name: "IPO Trademarks & Patents (data.gov.uk)",
    category: "market_intelligence",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "Intellectual Property Office trademark filings, patent applications, and design rights. Brand and innovation activity signals — new market entrants and product categories.",
    baseUrl: "https://data.gov.uk",
  },
  {
    id: "insolvency_register",
    name: "Insolvency Service Register (data.gov.uk)",
    category: "market_intelligence",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Business insolvency, administration, and liquidation datasets. Market exit signals — competitors closing, supply chain gaps, distressed-asset opportunities.",
    baseUrl: "https://data.gov.uk",
  },

  // ── REGULATORY DIRECTORIES ───────────────────────────────────────────────
  {
    id: "fca_register",
    name: "FCA Financial Services Register",
    category: "regulatory_directories",
    cadence: "weekly",
    requiresKey: true,
    keyEnvVar: "FCA_API_KEY",
    live: true,
    description: "50,000+ FCA-regulated firms. B2B buyer directory for any business selling into financial services — software, compliance, HR, legal, office services, events.",
    baseUrl: "https://register.fca.org.uk/services/V0.1",
  },
  {
    id: "sra_register",
    name: "SRA Solicitors Register (data.gov.uk)",
    category: "regulatory_directories",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "Solicitors Regulation Authority registered law firms. Buyer directory for legal tech, CPD training, office services, and professional services suppliers.",
    baseUrl: "https://data.gov.uk",
  },
  {
    id: "dvla_vehicles",
    name: "DVLA Vehicle Licensing Statistics",
    category: "regulatory_directories",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "UK registered vehicles by make, body type, fuel, and postcode area. Demand signals for automotive accessories, EV charging, fleet services, and insurance.",
    baseUrl: "https://data.gov.uk",
  },
  {
    id: "dvsa_mot",
    name: "DVSA MOT Test Results",
    category: "regulatory_directories",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "Anonymised MOT test data — vehicle age and condition by area. Signals for garages, parts suppliers, fleet operators, and vehicle safety businesses.",
    baseUrl: "https://data.gov.uk",
  },
  {
    id: "mhra_register",
    name: "MHRA Medical Devices Register (data.gov.uk)",
    category: "regulatory_directories",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "MHRA registered medical devices and medicines. Healthcare supply chain buyer directory — medical equipment, lab supplies, pharmaceuticals.",
    baseUrl: "https://data.gov.uk",
  },

  // ── CREATIVE & CULTURAL ───────────────────────────────────────────────────
  {
    id: "arts_council",
    name: "Arts Council England Grants (data.gov.uk)",
    category: "creative_cultural",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "Arts Council England grant funding datasets. Cultural activity signals for event planners, creatives, venues, fashion designers, and media businesses.",
    baseUrl: "https://data.gov.uk",
  },
  {
    id: "bfi_data",
    name: "BFI Film & TV Data (data.gov.uk)",
    category: "creative_cultural",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "British Film Institute industry datasets — productions, admissions, screen counts. Signals for location services, catering, costume, and production supply chains.",
    baseUrl: "https://data.gov.uk",
  },
  {
    id: "ofcom_data",
    name: "Ofcom Media & Communications Data",
    category: "creative_cultural",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "Ofcom internet usage, streaming habits, device adoption, and broadcast data. Signals for content creators, media businesses, tech companies, and advertisers.",
    baseUrl: "https://data.gov.uk",
  },

  // ── DEVOLVED NATIONS ─────────────────────────────────────────────────────
  {
    id: "stats_wales",
    name: "StatsWales API",
    category: "devolved_nations",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "Welsh Government statistical datasets — population, economy, health, business activity. Fills the Wales-specific gap in ONS/Nomis coverage.",
    baseUrl: "https://statswales.gov.wales/api/v1",
  },
  {
    id: "nisra",
    name: "NISRA (Northern Ireland Statistics)",
    category: "devolved_nations",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "Northern Ireland Statistics and Research Agency datasets. Fills the NI-specific gap in UK-wide data coverage.",
    baseUrl: "https://data.gov.uk",
  },
  {
    id: "scottish_gov_stats",
    name: "Scottish Government Statistics",
    category: "devolved_nations",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "Scottish national statistics via data.gov.uk and statistics.gov.scot SPARQL. Population, economy, and business data for Scotland-specific signals.",
    baseUrl: "https://statistics.gov.scot",
  },

  // ── MISSING 6 — closes gap to 43 ─────────────────────────────────────────

  // ONS core — already used in early-signals; register here for ingest tracking
  {
    id: "ons",
    name: "ONS Construction & Business Data",
    category: "government_public",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "ONS construction output + business demography indicators. Used for early-signal generation.",
    baseUrl: "https://api.beta.ons.gov.uk/v1",
  },

  // GOV.UK content — already used in early-signals; register here for ingest tracking
  {
    id: "govuk_content",
    name: "GOV.UK Content API",
    category: "government_public",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "GOV.UK content and policy announcements relevant to procurement sectors.",
    baseUrl: "https://www.gov.uk/api/content",
  },

  // Public Contracts Scotland
  {
    id: "public_contracts_scotland",
    name: "Public Contracts Scotland",
    category: "procurement_contracts",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Scottish Government procurement portal. RSS notice feed.",
    baseUrl: "https://www.publiccontractsscotland.gov.uk",
  },

  // eSourcing NI — Northern Ireland
  {
    id: "esourcing_ni",
    name: "eSourcing NI",
    category: "procurement_contracts",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Northern Ireland public procurement notices. Indexed via data.gov.uk CKAN.",
    baseUrl: "https://data.gov.uk",
  },

  // CQC
  {
    id: "cqc",
    name: "Care Quality Commission",
    category: "health_education_social",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "CQC registered providers and inspection outcomes. Free public API.",
    baseUrl: "https://api.cqc.org.uk/public/v1",
  },

  // Ofsted
  {
    id: "ofsted",
    name: "Ofsted Inspection Data",
    category: "health_education_social",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "Ofsted school and provider inspection datasets. Indexed via data.gov.uk.",
    baseUrl: "https://data.gov.uk",
  },

  // ── REAL-FIGURE PARSERS ─────────────────────────────────────────────────
  {
    id: "land_registry_transactions",
    name: "Land Registry Price Paid",
    category: "market_intelligence",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description:
      "HM Land Registry monthly price paid CSV — actual transaction counts and average prices by county and property type.",
    baseUrl:
      "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com",
  },
  {
    id: "ons_card_spending",
    name: "ONS UK Card Spending",
    category: "consumer_demand",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description:
      "ONS experimental weekly card spending index by category (Social, Staple, Discretionary). YoY change computed from CSV download.",
    baseUrl: "https://api.beta.ons.gov.uk/v1",
  },
  {
    id: "ch_new_businesses",
    name: "Companies House New Registrations",
    category: "market_intelligence",
    cadence: "daily",
    requiresKey: true,
    keyEnvVar: "COMPANIES_HOUSE_API_KEY",
    live: true,
    description:
      "New company incorporations by SIC sector and region via Companies House advanced search API.",
    baseUrl: "https://api.company-information.service.gov.uk",
  },
  {
    id: "dvla_vehicle_stats",
    name: "DVLA Vehicle Licensing Statistics",
    category: "market_intelligence",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description:
      "DVLA headline vehicle registration and licensing figures parsed from GOV.UK statistical publication — total fleet, new registrations, zero-emission counts.",
    baseUrl: "https://www.gov.uk/government/statistics",
  },
  {
    id: "dvla_ods_regional",
    name: "DVLA Regional Vehicle Fleet (ODS)",
    category: "market_intelligence",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description:
      "DVLA VEH0105 ODS — licensed cars by region, fuel type, and keepership. Real counts by English region + devolved nations, latest quarter.",
    baseUrl: "https://assets.publishing.service.gov.uk",
  },
  {
    id: "fsa_food_businesses",
    name: "FSA Food Business Register",
    category: "market_intelligence",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description:
      "FSA food establishment counts by city and business type (restaurants, takeaways, pubs, cafes) via the Food Standards Agency Establishments API.",
    baseUrl: "https://api.ratings.food.gov.uk",
  },
];

export function getSource(id: string): DataSource | undefined {
  return DATA_SOURCES.find(s => s.id === id);
}

export function getSourcesByCategory(category: SourceCategory): DataSource[] {
  return DATA_SOURCES.filter(s => s.category === category);
}

export function getLiveSources(): DataSource[] {
  return DATA_SOURCES.filter(s => s.live);
}
