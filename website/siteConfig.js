const repoUrl = "https://github.com/typelevel/fs2-kafka";

const apiUrl = "/fs2-kafka/api/fs2/kafka/index.html";

// See https://docusaurus.io/docs/site-config for available options.
const siteConfig = {
  title: "FS2 Kafka",
  tagline: "Functional Kafka Streams for Scala",
  url: "https://typelevel/fs2-kafka",
  baseUrl: "/fs2-kafka/",

  customDocsPath: "docs/target/mdoc",

  projectName: "fs2-kafka",
  organizationName: "typelevel",

  headerLinks: [
    { href: apiUrl, label: "API Docs" },
    { doc: "overview", label: "Documentation" },
    { href: repoUrl, label: "GitHub" },
  ],

  headerIcon: "img/fs2-kafka.white.svg",
  titleIcon: "img/fs2-kafka.svg",
  favicon: "img/favicon.png",

  colors: {
    primaryColor: "#122932",
    secondaryColor: "#153243",
  },

  copyright: `Copyright © 2018 OVO Energy Limited.`,

  highlight: { theme: "github" },

  onPageNav: "separate",

  separateCss: ["api"],

  cleanUrl: true,

  repoUrl,

  apiUrl,
};

module.exports = siteConfig;
